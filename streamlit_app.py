import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
import warnings
import io
import hashlib
import pickle
import os
import time
import re
warnings.filterwarnings('ignore')

# ============================================
# 1. КОНФИГУРАЦИЯ И КОНСТАНТЫ
# ============================================
DB_PATH = 'olap_analytics_pro.db'
APP_VERSION = "4.0.0 Enterprise"
MAX_UPLOAD_SIZE_MB = 100

st.set_page_config(
    page_title=f"OLAP Analytics Pro v{APP_VERSION}",
    page_icon="🎲",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://duckdb.org/docs/',
        'Report a bug': "https://github.com/duckdb/duckdb/issues",
        'About': f"# OLAP Analytics Pro {APP_VERSION}\nМощная платформа для многомерного анализа данных."
    }
)

# ============================================
# 2. УПРАВЛЕНИЕ СОСТОЯНИЕМ (SESSION STATE)
# ============================================
def init_session_state():
    """Инициализация переменных сессии по умолчанию"""
    defaults = {
        'authenticated': False,
        'username': None,
        'role': None,
        'user_id': None,
        'full_name': None,
        'current_cube': None,
        'drill_path': [],
        'filters': {},
        'pivot_config': {'rows': [], 'cols': [], 'measures': []},
        'chart_history': [],
        'db_ready': False,
        'theme_accent': '#1e3c72'
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            # Для изменяемых типов создаем новые экземпляры
            st.session_state[k] = v if not isinstance(v, (list, dict)) else type(v)()

init_session_state()

# ============================================
# 3. ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ (SINGLETON)
# ============================================
@st.cache_resource
def get_db_connection():
    """
    Создает единственное подключение к DuckDB с оптимизациями.
    Использует кэширование ресурсов Streamlit для сохранения состояния между перезагрузками.
    """
    conn = duckdb.connect(DB_PATH)
    # Установка расширений
    conn.execute("INSTALL json; LOAD json;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    # Настройки производительности
    conn.execute("SET memory_limit='4GB';")
    conn.execute("SET threads=4;")
    conn.execute("SET enable_object_cache=true;")
    return conn

conn = get_db_connection()

# ============================================
# 4. УТИЛИТЫ БЕЗОПАСНОСТИ И АУДИТА
# ============================================

def _safe_sql_identifier(name: str) -> str:
    """
    Безопасное экранирование имен таблиц и колонок (защита от SQL injection в именах).
    Пример: my"table -> "my""table"
    """
    return f'"{str(name).replace("\"", "\"\"")}"'

def _safe_sql_value(value: Any) -> str:
    """
    Безопасное форматирование значений для вставки в SQL строку.
    Обрабатывает None, bool, числа и строки (экранируя одинарные кавычки).
    Исправленная версия без синтаксических ошибок.
    """
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, (int, float)):
        return str(value)
    
    # Для строк: экранируем одинарные кавычки удвоением (стандарт SQL)
    s = str(value).replace("'", "''")
    return f"'{s}'"

def log_audit(action: str, details: Dict = None):
    """Запись действия пользователя в журнал аудита"""
    try:
        username = st.session_state.get('username', 'system')
        user_id = st.session_state.get('user_id')
        
        # Проверка существования таблицы перед записью
        tables = conn.execute("SHOW TABLES").fetchdf()
        if 'audit_log' not in tables['table_name'].values:
            return
            
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM audit_log").fetchone()[0]
        conn.execute("""
            INSERT INTO audit_log (id, user_name, user_id, action, details, timestamp)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [max_id + 1, username, user_id, action, json.dumps(details or {}, default=str)])
    except Exception:
        pass # Не прерываем работу приложения при ошибке логирования

# ============================================
# 5. ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
# ============================================
def ensure_database_initialized():
    """Проверяет наличие таблиц и создает их при необходимости"""
    if st.session_state.db_ready:
        return True

    try:
        # Проверяем наличие ключевой таблицы
        tables = conn.execute("SHOW TABLES").fetchdf()['table_name'].tolist()
        
        if 'users' not in tables:
            create_schema()
            seed_default_data()
            st.sidebar.success("🆕 База данных успешно создана!")
        else:
            migrate_schema_if_needed()
            
        st.session_state.db_ready = True
        return True
    except Exception as e:
        st.error(f"❌ Критическая ошибка инициализации БД: {e}")
        return False

def create_schema():
    """Создание полной схемы реляционных таблиц"""
    schema_sql = [
        """CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR UNIQUE NOT NULL,
            password_hash VARCHAR NOT NULL,
            role VARCHAR DEFAULT 'VIEWER',
            email VARCHAR,
            full_name VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        )""",
        """CREATE TABLE permissions (
            id INTEGER PRIMARY KEY,
            role VARCHAR NOT NULL,
            resource_type VARCHAR DEFAULT 'CUBE', -- Тип ресурса (CUBE, DASHBOARD)
            resource_name VARCHAR, -- Имя ресурса (* для всех)
            access_level VARCHAR DEFAULT 'READ', -- READ, WRITE, ADMIN
            UNIQUE(role, resource_type, resource_name)
        )""",
        """CREATE TABLE olap_cubes (
            id INTEGER PRIMARY KEY,
            name VARCHAR UNIQUE NOT NULL,
            source_table VARCHAR NOT NULL,
            definition JSON, -- JSON с описанием измерений и мер
            row_count BIGINT DEFAULT 0,
            owner VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE olap_slices (
            id INTEGER PRIMARY KEY,
            cube_name VARCHAR,
            name VARCHAR,
            config JSON, -- Сохраненные фильтры и настройки
            owner VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE dashboards (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            cube_name VARCHAR,
            layout JSON,
            owner VARCHAR,
            is_public BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE query_history (
            id INTEGER PRIMARY KEY,
            user_name VARCHAR,
            cube_name VARCHAR,
            query_hash VARCHAR,
            execution_time_ms FLOAT,
            rows_returned INTEGER,
            status VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            user_name VARCHAR,
            user_id INTEGER,
            action VARCHAR,
            details JSON,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    ]
    
    for sql in schema_sql:
        try:
            conn.execute(sql)
        except Exception as e:
            print(f"Предупреждение при создании таблицы: {e}")

def seed_default_data():
    """Заполнение БД начальными данными (админ, аналитик, зритель)"""
    import hashlib
    
    # Создаем демо-пользователей
    users = [
        (1, 'admin', hashlib.sha256('admin123'.encode()).hexdigest(), 'ADMIN', 'admin@sys.local', 'Администратор', True),
        (2, 'analyst', hashlib.sha256('analyst123'.encode()).hexdigest(), 'ANALYST', 'analyst@sys.local', 'Аналитик', True),
        (3, 'viewer', hashlib.sha256('viewer123'.encode()).hexdigest(), 'VIEWER', 'viewer@sys.local', 'Гость', True)
    ]
    
    for u in users:
        conn.execute("""
            INSERT INTO users (id, username, password_hash, role, email, full_name, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, u)
        
    # Права доступа по умолчанию
    perms = [
        (1, 'ADMIN', 'CUBE', '*', 'ADMIN'),
        (2, 'ANALYST', 'CUBE', '*', 'WRITE'),
        (3, 'VIEWER', 'CUBE', '*', 'READ')
    ]
    for p in perms:
        conn.execute("INSERT INTO permissions VALUES (?,?,?,?,?)", p)

def migrate_schema_if_needed():
    """Добавление новых колонок при обновлении версии приложения"""
    try:
        cols = conn.execute("PRAGMA table_info(users)").fetchdf()['name'].tolist()
        if 'full_name' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN full_name VARCHAR")
        if 'is_active' not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
    except Exception:
        pass

# Запуск проверки БД
ensure_database_initialized()

# ============================================
# 6. CSS СТИЛИ (ENTERPRISE THEME)
# ============================================
st.markdown("""
<style>
    :root {
        --primary-color: #1e3c72;
        --secondary-color: #2a5298;
        --accent-color: #667eea;
        --bg-light: #f8f9fa;
        --text-dark: #2c3e50;
    }
    
    /* Основной заголовок */
    .main-header {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    }
    
    .main-header h1 { margin: 0; font-size: 2.2rem; font-weight: 700; }
    .main-header p { margin: 5px 0 0; opacity: 0.9; }
    
    /* Карточки метрик */
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        border-left: 4px solid var(--accent-color);
        transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-3px); }
    
    .kpi-value { font-size: 2.5rem; font-weight: bold; color: var(--primary-color); }
    .kpi-label { color: #666; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; }
    
    /* Бейджи ролей */
    .badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
        color: white;
    }
    .badge-admin { background-color: #e74c3c; }
    .badge-analyst { background-color: #3498db; }
    .badge-viewer { background-color: #2ecc71; }
    
    /* Таблицы данных */
    .dataframe th {
        background-color: var(--primary-color) !important;
        color: white !important;
        font-weight: 600;
    }
    
    /* Боковая панель */
    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #eee;
    }
    
    /* Кнопки */
    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transform: translateY(-1px);
    }
    
    /* Вкладки */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [aria-selected="true"] {
        background-color: var(--primary-color) !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 7. ЯДРО OLAP (МОДЕЛИ ДАННЫХ И ДВИЖОК)
# ============================================

class OLAPDimension:
    """Класс измерения OLAP куба"""
    def __init__(self, name, column, data_type='string', hierarchy=None):
        self.name = name
        self.column = column
        self.data_type = data_type # string, number, datetime
        self.hierarchy = hierarchy or [] # Например: ['Год', 'Месяц', 'День']
        
    def to_dict(self):
        return {'name': self.name, 'column': self.column, 'type': self.data_type, 'hierarchy': self.hierarchy}

class OLAPMeasure:
    """Класс меры (показателя) OLAP куба"""
    def __init__(self, name, column, agg_func='SUM', format_str='{:,.2f}'):
        self.name = name
        self.column = column
        self.agg_func = agg_func.upper() # SUM, AVG, COUNT, MIN, MAX
        self.format_str = format_str
        
    def to_dict(self):
        return {'name': self.name, 'column': self.column, 'agg': self.agg_func}

class OLAPCube:
    """Основной класс OLAP Куба"""
    def __init__(self, name, table_name, description=""):
        self.name = name
        self.table_name = table_name
        self.description = description
        self.dimensions: Dict[str, OLAPDimension] = {}
        self.measures: Dict[str, OLAPMeasure] = {}
        self.time_dimension: Optional[str] = None # Имя главного временного измерения
        
    def add_dimension(self, dim: OLAPDimension):
        self.dimensions[dim.name] = dim
        if dim.data_type == 'datetime':
            self.time_dimension = dim.name
            
    def add_measure(self, meas: OLAPMeasure):
        self.measures[meas.name] = meas

    def save_to_db(self):
        """Сохраняет метаданные куба в БД"""
        definition = {
            'dimensions': {k: v.to_dict() for k, v in self.dimensions.items()},
            'measures': {k: v.to_dict() for k, v in self.measures.items()},
            'time_dim': self.time_dimension
        }
        
        exists = conn.execute("SELECT COUNT(*) FROM olap_cubes WHERE name=?", [self.name]).fetchone()[0]
        user = st.session_state.get('username', 'system')
        
        if exists:
            conn.execute("""UPDATE olap_cubes SET definition=?, updated_at=CURRENT_TIMESTAMP 
                         WHERE name=?""", [json.dumps(definition, default=str), self.name])
        else:
            # Подсчет строк для статистики
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {_safe_sql_identifier(self.table_name)}").fetchone()[0]
            except:
                count = 0
                
            mid = conn.execute("SELECT COALESCE(MAX(id),0)+1 FROM olap_cubes").fetchone()[0]
            conn.execute("""INSERT INTO olap_cubes (id, name, source_table, definition, row_count, owner)
                         VALUES (?,?,?,?,?,?)""", 
                         [mid, self.name, self.table_name, json.dumps(definition, default=str), count, user])

class CubeEngine:
    """
    Движок выполнения запросов.
    Генерирует SQL на основе конфигурации куба и параметров пользователя.
    """
    
    def __init__(self, cube: OLAPCube):
        self.cube = cube
        self.conn = conn
        
    def generate_query(self, rows: List[str], cols: List[str], measures: List[str], 
                       filters: Dict, top_n: int = None) -> str:
        """Генерация SQL запроса с безопасной подстановкой"""
        
        # 1. Формирование SELECT
        select_cols = []
        group_cols = []
        
        # Добавляем измерения (Dimensions)
        all_dims = list(set(rows + cols))
        for d_name in all_dims:
            if d_name in self.cube.dimensions:
                dim = self.cube.dimensions[d_name]
                safe_col = _safe_sql_identifier(dim.column)
                alias = _safe_sql_identifier(d_name)
                select_cols.append(f"{safe_col} as {alias}")
                group_cols.append(safe_col)
                
        # Добавляем меры (Measures) с агрегацией
        for m_name in measures:
            if m_name in self.cube.measures:
                m = self.cube.measures[m_name]
                safe_col = _safe_sql_identifier(m.column)
                alias = _safe_sql_identifier(m_name)
                select_cols.append(f"{m.agg_func}({safe_col}) as {alias}")
        
        if not select_cols:
            return ""
            
        query = f"SELECT {', '.join(select_cols)} FROM {_safe_sql_identifier(self.cube.table_name)}"
        
        # 2. Формирование WHERE (Фильтры)
        where_clauses = []
        for col_name, val in filters.items():
            # Ищем оригинальное имя колонки по имени измерения
            orig_col = None
            for d in self.cube.dimensions.values():
                if d.name == col_name:
                    orig_col = d.column
                    break
            
            if orig_col:
                safe_orig = _safe_sql_identifier(orig_col)
                if isinstance(val, list):
                    if val:
                        # Используем _safe_sql_value для каждого элемента списка
                        vals = ', '.join([_safe_sql_value(v) for v in val])
                        where_clauses.append(f"{safe_orig} IN ({vals})")
                elif isinstance(val, dict):
                    # Диапазон (min/max)
                    if 'min' in val and val['min'] is not None:
                        where_clauses.append(f"{safe_orig} >= {_safe_sql_value(val['min'])}")
                    if 'max' in val and val['max'] is not None:
                        where_clauses.append(f"{safe_orig} <= {_safe_sql_value(val['max'])}")
                else:
                    # Равенство
                    if val is not None:
                        where_clauses.append(f"{safe_orig} = {_safe_sql_value(val)}")
                        
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"
            
        # 3. GROUP BY
        if group_cols:
            query += f" GROUP BY {', '.join(group_cols)}"
            
        # 4. ORDER BY & LIMIT
        if measures:
            # Сортировка по первой мере по убыванию
            first_m = _safe_sql_identifier(measures[0])
            query += f" ORDER BY {first_m} DESC"
            
        if top_n and top_n > 0:
            query += f" LIMIT {top_n}"
            
        return query

    def execute(self, rows: List[str], cols: List[str], measures: List[str], 
                filters: Dict, top_n: int = None) -> pd.DataFrame:
        """Выполняет сгенерированный запрос и возвращает DataFrame"""
        query = self.generate_query(rows, cols, measures, filters, top_n)
        
        if not query:
            return pd.DataFrame()
            
        start_time = time.time()
        try:
            df = self.conn.execute(query).fetchdf()
            exec_time = (time.time() - start_time) * 1000
            
            # Логирование производительности
            self._log_perf(query, exec_time, len(df))
            
            return df
        except Exception as e:
            st.error(f"Ошибка выполнения запроса: {e}")
            return pd.DataFrame()
            
    def _log_perf(self, query, time_ms, rows):
        """Запись статистики запроса"""
        try:
            h = hashlib.md5(query.encode()).hexdigest()[:16]
            mid = self.conn.execute("SELECT COALESCE(MAX(id),0)+1 FROM query_history").fetchone()[0]
            self.conn.execute("""INSERT INTO query_history 
                              (id, user_name, cube_name, query_hash, execution_time_ms, rows_returned, status)
                              VALUES (?,?,?,?,?,?,?)""",
                              [mid, st.session_state.username, self.cube.name, h, time_ms, rows, 'SUCCESS'])
        except:
            pass

    def get_time_series(self, measure_name: str, freq: str = 'month') -> pd.DataFrame:
        """Специальный метод для получения временных рядов с авто-агрегацией по датам"""
        if not self.cube.time_dimension:
            return pd.DataFrame()
            
        dim = self.cube.dimensions[self.cube.time_dimension]
        m = self.cube.measures[measure_name]
        
        date_col = _safe_sql_identifier(dim.column)
        meas_col = _safe_sql_identifier(m.column)
        
        # Использование DATE_TRUNC DuckDB для группировки по времени
        query = f"""
            SELECT 
                DATE_TRUNC('{freq}', {date_col}) as period,
                {m.agg_func}({meas_col}) as value
            FROM {_safe_sql_identifier(self.cube.table_name)}
            GROUP BY 1
            ORDER BY 1
        """
        return self.conn.execute(query).fetchdf()

# ============================================
# 8. МЕНЕДЖЕР ПОЛЬЗОВАТЕЛЕЙ
# ============================================
class UserManager:
    @staticmethod
    def login(username, password):
        """Аутентификация пользователя"""
        if not username or not password:
            return False, "Введите логин и пароль"
            
        pwd_hash = hashlib.sha256(password.encode()).hexdigest()
        
        try:
            res = conn.execute("""
                SELECT id, username, role, is_active, full_name 
                FROM users 
                WHERE LOWER(username) = LOWER(?) AND password_hash = ?
            """, [username.strip(), pwd_hash]).fetchone()
            
            if not res:
                # Проверка: пользователь не найден или неверный пароль
                user_exists = conn.execute("SELECT 1 FROM users WHERE LOWER(username)=LOWER(?)", [username]).fetchone()
                if user_exists:
                    log_audit("LOGIN_FAIL", {"user": username, "reason": "bad_password"})
                    return False, "Неверный пароль"
                else:
                    log_audit("LOGIN_FAIL", {"user": username, "reason": "not_found"})
                    return False, "Пользователь не найден"
                    
            uid, uname, role, active, fname = res
            
            if not active:
                return False, "Аккаунт заблокирован администратором"
                
            # Успешный вход
            conn.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id=?", [uid])
            st.session_state.authenticated = True
            st.session_state.username = uname
            st.session_state.role = role
            st.session_state.user_id = uid
            st.session_state.full_name = fname
            
            log_audit("LOGIN_SUCCESS", {"user": uname})
            return True, "OK"
            
        except Exception as e:
            return False, f"Системная ошибка: {e}"

    @staticmethod
    def check_access(resource_type, resource_name, level='READ'):
        """Проверка прав доступа"""
        if not st.session_state.authenticated:
            return False
        if st.session_state.role == 'ADMIN':
            return True
            
        # Проверка конкретных прав или глобальных (*)
        res = conn.execute("""
            SELECT access_level FROM permissions
            WHERE role = ? AND resource_type = ? 
            AND (resource_name = ? OR resource_name = '*')
            LIMIT 1
        """, [st.session_state.role, resource_type, resource_name]).fetchone()
        
        if not res:
            return False
            
        levels = {'READ': 1, 'WRITE': 2, 'ADMIN': 3}
        return levels.get(res[0], 0) >= levels.get(level, 1)

# ============================================
# 9. UI КОМПОНЕНТЫ И СТРАНИЦЫ
# ============================================

def render_login():
    """Страница входа"""
    st.markdown("""
    <div style="display:flex; justify-content:center; align-items:center; height:80vh;">
        <div style="background:white; padding:40px; border-radius:15px; box-shadow:0 10px 25px rgba(0,0,0,0.1); width:100%; max-width:400px;">
            <h2 style="text-align:center; color:#1e3c72; margin-bottom:30px;">🔐 Вход в систему</h2>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.markdown("<h3 style='text-align:center'>Авторизация</h3>", unsafe_allow_html=True)
        u = st.text_input("Логин", placeholder="admin", key="login_user")
        p = st.text_input("Пароль", type="password", placeholder="••••••", key="login_pass")
        
        if st.button("Войти", type="primary", use_container_width=True):
            success, msg = UserManager.login(u, p)
            if success:
                st.success("Успешный вход!")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error(msg)
                    
        st.markdown("---")
        st.caption("Демо доступ: admin/admin123 | analyst/analyst123")

def render_header():
    """Верхняя панель приложения"""
    role_class = f"badge-{st.session_state.role.lower()}"
    st.markdown(f"""
    <div class="main-header">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <div>
                <h1>🎲 OLAP Analytics Pro</h1>
                <p>Enterprise Edition v{APP_VERSION}</p>
            </div>
            <div style="text-align:right;">
                <span style="font-size:1.1rem;">👤 {st.session_state.full_name or st.session_state.username}</span><br>
                <span class="badge {role_class}">{st.session_state.role}</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def load_cube(name):
    """Загрузка метаданных куба из БД в объект OLAPCube"""
    res = conn.execute("SELECT source_table, definition FROM olap_cubes WHERE name=?", [name]).fetchone()
    if not res:
        return
        
    table_name, def_json = res
    def_data = json.loads(def_json)
    
    cube = OLAPCube(name, table_name)
    
    # Восстановление измерений
    for d_name, d_data in def_data.get('dimensions', {}).items():
        dim = OLAPDimension(d_name, d_data['column'], d_data.get('type', 'string'), d_data.get('hierarchy'))
        cube.add_dimension(dim)
        
    # Восстановление мер
    for m_name, m_data in def_data.get('measures', {}).items():
        meas = OLAPMeasure(m_name, m_data['column'], m_data.get('agg', 'SUM'))
        cube.add_measure(meas)
        
    cube.time_dimension = def_data.get('time_dim')
    st.session_state.current_cube = cube
    st.session_state.filters = {} # Сброс фильтров при смене куба

def sidebar_controls():
    """Боковая панель навигации"""
    with st.sidebar:
        st.markdown("### 🧭 Навигация")
        page = st.radio("", [
            "📊 Анализ и Дашборды",
            "🏗️ Конструктор Кубов",
            "💾 Сохраненные Срезы",
            "⚙️ Администрирование"
        ], label_visibility="collapsed")
        
        st.divider()
        
        # Выбор активного куба
        st.markdown("### 📦 Активный Куб")
        cubes_df = conn.execute("SELECT name FROM olap_cubes ORDER BY name").fetchdf()
        
        if not cubes_df.empty:
            cube_names = cubes_df['name'].tolist()
            # Если текущий куб есть в списке, выбираем его, иначе первый
            idx = 0
            if st.session_state.current_cube and st.session_state.current_cube.name in cube_names:
                idx = cube_names.index(st.session_state.current_cube.name)
                
            selected = st.selectbox("Выберите куб", cube_names, index=idx, key="sidebar_cube_sel")
            
            if st.button("Загрузить куб", use_container_width=True):
                load_cube(selected)
        else:
            st.info("Нет созданных кубов. Перейдите в Конструктор.")
            
        if st.session_state.current_cube:
            st.success(f"✅ {st.session_state.current_cube.name}")
            # Получение количества строк из метаданных
            meta = conn.execute("SELECT row_count FROM olap_cubes WHERE name=?", [st.session_state.current_cube.name]).fetchone()
            if meta:
                st.caption(f"Записей: {meta[0]:,}".replace(",", " "))
        
        st.divider()
        if st.button("🚪 Выход", use_container_width=True, type="secondary"):
            for k in ['authenticated', 'username', 'role', 'user_id', 'current_cube', 'full_name']:
                st.session_state.pop(k, None)
            st.rerun()
            
    return page

# ============================================
# 10. ОСНОВНЫЕ РЕЖИМЫ РАБОТЫ
# ============================================

def render_analysis_mode():
    """Режим анализа данных (сводные таблицы и графики)"""
    if not st.session_state.current_cube:
        st.warning("⚠️ Пожалуйста, выберите и загрузите куб в боковом меню.")
        return

    cube = st.session_state.current_cube
    engine = CubeEngine(cube)
    
    tab1, tab2, tab3 = st.tabs(["📊 Сводная таблица", "📈 Визуализация", "🔍 Детальные данные"])
    
    # --- Панель фильтров ---
    with st.expander("🔍 Фильтры данных", expanded=False):
        filter_cols = st.columns(min(len(cube.dimensions), 4))
        for i, (d_name, dim) in enumerate(cube.dimensions.items()):
            with filter_cols[i % len(filter_cols)]:
                # Получаем уникальные значения для фильтра (лимит 1000 для скорости)
                try:
                    vals = conn.execute(f"SELECT DISTINCT {_safe_sql_identifier(dim.column)} FROM {_safe_sql_identifier(cube.table_name)} LIMIT 1000").fetchdf()[dim.column].tolist()
                    selected = st.multiselect(dim.name, vals, default=st.session_state.filters.get(d_name, []), key=f"filt_{d_name}")
                    if selected:
                        st.session_state.filters[d_name] = selected
                    elif d_name in st.session_state.filters:
                        del st.session_state.filters[d_name]
                except:
                    pass
        
        if st.button("Применить фильтры", type="primary"):
            st.rerun()

    # --- Вкладка 1: Сводная таблица ---
    with tab1:
        st.subheader("Конфигуратор сводной таблицы")
        c1, c2, c3 = st.columns(3)
        with c1:
            rows = st.multiselect("Строки", list(cube.dimensions.keys()), key="pivot_rows")
        with c2:
            cols = st.multiselect("Колонки", list(cube.dimensions.keys()), key="pivot_cols")
        with c3:
            measures = st.multiselect("Показатели (Меры)", list(cube.measures.keys()), default=list(cube.measures.keys())[:1], key="pivot_meas")
            
        top_n = st.slider("Топ N записей", 0, 5000, 100)
        
        if st.button("Построить отчет", type="primary"):
            with st.spinner("Выполнение запроса..."):
                df = engine.execute(rows, cols, measures, st.session_state.filters, top_n)
                
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                    
                    # Кнопки экспорта
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 Скачать CSV", csv, "report.csv", "text/csv")
                else:
                    st.info("Нет данных для выбранных критериев.")

    # --- Вкладка 2: Визуализация ---
    with tab2:
        if not cube.time_dimension and len(cube.dimensions) < 1:
            st.warning("Для построения графиков нужны измерения.")
        else:
            chart_type = st.selectbox("Тип графика", ["Bar Chart", "Line Chart", "Pie Chart", "Area Chart"])
            dim_x = st.selectbox("Измерение (Ось X / Категории)", list(cube.dimensions.keys()))
            meas_y = st.selectbox("Показатель (Ось Y)", list(cube.measures.keys()))
            
            if st.button("Построить график"):
                df = engine.execute([dim_x], [], [meas_y], st.session_state.filters, top_n=50)
                
                if not df.empty:
                    fig = None
                    if chart_type == "Bar Chart":
                        fig = px.bar(df, x=dim_x, y=meas_y, title=f"{meas_y} по {dim_x}", color=meas_y)
                    elif chart_type == "Line Chart":
                        fig = px.line(df, x=dim_x, y=meas_y, title=f"Динамика: {meas_y}", markers=True)
                    elif chart_type == "Pie Chart":
                        fig = px.pie(df, names=dim_x, values=meas_y, title=f"Доля: {meas_y}")
                    elif chart_type == "Area Chart":
                        fig = px.area(df, x=dim_x, y=meas_y, title=f"Объем: {meas_y}")
                        
                    if fig:
                        fig.update_layout(template="plotly_white")
                        st.plotly_chart(fig, use_container_width=True)

    # --- Вкладка 3: Детальные данные ---
    with tab3:
        st.subheader("Просмотр сырых данных")
        limit = st.number_input("Лимит строк", value=100, max_value=10000)
        if st.button("Показать данные"):
            # Простой SELECT без агрегации
            q = f"SELECT * FROM {_safe_sql_identifier(cube.table_name)} LIMIT {limit}"
            try:
                df_raw = conn.execute(q).fetchdf()
                st.dataframe(df_raw, use_container_width=True)
            except Exception as e:
                st.error(f"Ошибка: {e}")

def render_builder_mode():
    """Режим создания новых кубов (ETL)"""
    if not UserManager.check_access('CUBE', '*', 'WRITE'):
        st.error("🚫 Недостаточно прав для создания кубов.")
        return

    st.header("🏗️ Конструктор OLAP Кубов")
    st.markdown("Загрузите данные (CSV, Excel, Parquet) для создания нового аналитического куба.")
    
    uploaded_file = st.file_uploader("Выберите файл", type=['csv', 'xlsx', 'parquet'])
    
    if uploaded_file:
        try:
            # Чтение файла
            if uploaded_file.name.endswith('csv'):
                df = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith('xlsx'):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_parquet(uploaded_file)
                
            st.success(f"Файл загружен: {len(df)} строк, {len(df.columns)} колонок.")
            
            # Настройка куба
            with st.form("create_cube_form"):
                cube_name = st.text_input("Название куба", value=uploaded_file.name.split('.')[0])
                cube_desc = st.text_area("Описание")
                
                st.markdown("#### Определение структуры")
                c1, c2 = st.columns(2)
                
                # Автоопределение типов
                dims_candidates = [c for c in df.columns if df[c].dtype == 'object' or pd.api.types.is_datetime64_any_dtype(df[c])]
                meas_candidates = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                
                with c1:
                    sel_dims = st.multiselect("Выберите Измерения (Группировки)", df.columns, default=dims_candidates[:3])
                with c2:
                    sel_measures = st.multiselect("Выберите Показатели (Числа)", df.columns, default=meas_candidates[:2])
                
                submitted = st.form_submit_button("🚀 Создать Куб")
                
                if submitted:
                    if not sel_dims or not sel_measures:
                        st.error("Выберите хотя бы одно измерение и один показатель.")
                    else:
                        # Создание таблицы в DuckDB
                        table_name = f"cube_{hashlib.md5(cube_name.encode()).hexdigest()[:8]}"
                        
                        # Регистрация DF и создание таблицы
                        conn.register("upload_df", df)
                        conn.execute(f"CREATE OR REPLACE TABLE {_safe_sql_identifier(table_name)} AS SELECT * FROM upload_df")
                        conn.unregister("upload_df")
                        
                        # Создание объекта куба
                        new_cube = OLAPCube(cube_name, table_name, cube_desc)
                        
                        # Добавление измерений
                        for d in sel_dims:
                            dtype = 'datetime' if pd.api.types.is_datetime64_any_dtype(df[d]) else 'string'
                            new_cube.add_dimension(OLAPDimension(d, d, dtype))
                            
                        # Добавление мер
                        for m in sel_measures:
                            new_cube.add_measure(OLAPMeasure(m, m, 'SUM'))
                            
                        # Сохранение метаданных
                        new_cube.save_to_db()
                        
                        st.success(f"✅ Куб '{cube_name}' успешно создан!")
                        load_cube(cube_name)
                        st.rerun()
                        
        except Exception as e:
            st.error(f"Ошибка обработки файла: {e}")

def render_admin_mode():
    """Панель администратора"""
    if st.session_state.role != 'ADMIN':
        st.error("🚫 Доступ запрещен.")
        return
        
    st.header("⚙️ Администрирование системы")
    
    t1, t2, t3 = st.tabs(["👥 Пользователи", "📜 Журнал Аудита", "🗄️ Система"])
    
    with t1:
        st.subheader("Управление пользователями")
        users_df = conn.execute("SELECT id, username, role, full_name, is_active, last_login FROM users").fetchdf()
        st.dataframe(users_df, use_container_width=True)
        
        with st.expander("➕ Добавить пользователя"):
            with st.form("add_user"):
                u_name = st.text_input("Логин")
                u_pass = st.text_input("Пароль", type="password")
                u_role = st.selectbox("Роль", ['VIEWER', 'ANALYST', 'ADMIN'])
                u_full = st.text_input("Полное имя")
                if st.form_submit_button("Создать"):
                    if u_name and u_pass:
                        h = hashlib.sha256(u_pass.encode()).hexdigest()
                        try:
                            nid = conn.execute("SELECT COALESCE(MAX(id),0)+1 FROM users").fetchone()[0]
                            conn.execute("INSERT INTO users (id, username, password_hash, role, full_name) VALUES (?,?,?,?,?)",
                                         [nid, u_name, h, u_role, u_full])
                            st.success("Пользователь создан")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка: {e}")

    with t2:
        st.subheader("Журнал действий")
        logs = conn.execute("SELECT timestamp, user_name, action, details FROM audit_log ORDER BY timestamp DESC LIMIT 100").fetchdf()
        st.dataframe(logs, use_container_width=True)

    with t3:
        st.subheader("Состояние системы")
        if st.button("🧹 Очистить историю запросов"):
            conn.execute("DELETE FROM query_history")
            st.success("История очищена")
            
        size = os.path.getsize(DB_PATH) / (1024*1024)
        st.metric("Размер файла БД", f"{size:.2f} MB")

# ============================================
# 11. ГЛАВНАЯ ТОЧКА ВХОДА
# ============================================
def main():
    if not st.session_state.authenticated:
        render_login()
    else:
        render_header()
        page = sidebar_controls()
        
        if page == "📊 Анализ и Дашборды":
            render_analysis_mode()
        elif page == "🏗️ Конструктор Кубов":
            render_builder_mode()
        elif page == "💾 Сохраненные Срезы":
            st.info("Функционал сохраненных срезов находится в разработке.")
        elif page == "⚙️ Администрирование":
            render_admin_mode()

if __name__ == "__main__":
    main()
