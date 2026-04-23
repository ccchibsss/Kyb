import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import warnings
import io
import hashlib
import pickle
import os
import base64
import time
warnings.filterwarnings('ignore')

# ============================================
# 1. НАСТРОЙКА СТРАНИЦЫ
# ============================================
st.set_page_config(
    page_title="OLAP Analytics Pro",
    page_icon="🎲",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/olap-analytics',
        'Report a bug': 'https://github.com/olap-analytics/issues',
        'About': "# OLAP Analytics Pro v3.0\nМногомерный анализ данных на DuckDB + Streamlit"
    }
)

# ============================================
# 2. КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# ============================================
DB_PATH = 'olap_analytics.db'
APP_VERSION = "3.0.1"

# Гарантированная инициализация session_state ДО любого использования
def _init_session_state():
    """Инициализация session_state с безопасными дефолтами"""
    defaults = {
        'authenticated': False,
        'username': None,
        'role': None,
        'user_id': None,
        'user_email': None,
        'user_fullname': None,
        'current_cube': None,
        'drill_path': [],
        'filters': {},
        'pivot_rows': [],
        'pivot_cols': [],
        'pivot_measures': [],
        'selected_dimensions': [],
        'selected_measures': [],
        'chart_figures': [],
        'last_activity': datetime.now(),
        'db_initialized': False,
        'login_attempts': {},
        'theme': 'light'
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            if isinstance(value, (list, dict)):
                st.session_state[key] = type(value)()
            else:
                st.session_state[key] = value

_init_session_state()

# ============================================
# 3. ПОДКЛЮЧЕНИЕ К БД С ПОВТОРНЫМИ ПОПЫТКАМИ
# ============================================
@st.cache_resource
def get_connection(max_retries: int = 3, retry_delay: float = 0.5):
    """Подключение к DuckDB с обработкой временных ошибок"""
    last_error = None
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DB_PATH)
            # Расширения в DuckDB часто встроены, но попытка загрузить не помешает
            try:
                conn.execute("INSTALL json; LOAD json;")
            except:
                pass
            try:
                conn.execute("INSTALL httpfs; LOAD httpfs;")
            except:
                pass
            # Настройки производительности
            conn.execute("SET memory_limit='2GB'")
            conn.execute("SET threads=4")
            return conn
        except duckdb.Error as e:
            last_error = e
            time.sleep(retry_delay * (attempt + 1))
        except Exception as e:
            last_error = e
            break
    raise RuntimeError(f"Не удалось подключиться к БД после {max_retries} попыток: {last_error}")

conn = get_connection()

# ============================================
# 4. ФУНКЦИЯ АУДИТА С БЕЗОПАСНОЙ ОБРАБОТКОЙ
# ============================================
def log_audit(action: str, details: Dict = None):
    """Безопасное логирование действий пользователя"""
    try:
        username = st.session_state.get('username', 'system')
        user_id = st.session_state.get('user_id')
        ip_address = "local"  # В продакшене: st.context.headers.get("X-Forwarded-For", "unknown")
        
        # Проверяем существование таблицы перед записью
        tables = conn.execute("SHOW TABLES").fetchdf()
        if 'audit_log' not in tables['name'].values:
            return
            
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM audit_log").fetchone()[0]
        conn.execute("""
            INSERT INTO audit_log (id, user_name, user_id, action, details, ip_address)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [max_id + 1, username, user_id, action, json.dumps(details or {}, default=str), ip_address])
    except Exception:
        # Не прерываем работу приложения при ошибке аудита
        pass

# ============================================
# 5. БЕЗОПАСНАЯ ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
# ============================================
def _create_all_tables():
    """Создание всех таблиц схемы"""
    # ТАБЛИЦА ПОЛЬЗОВАТЕЛЕЙ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username VARCHAR NOT NULL UNIQUE,
            password_hash VARCHAR NOT NULL,
            role VARCHAR DEFAULT 'VIEWER',
            email VARCHAR,
            full_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    """)
    
    # ТАБЛИЦА ПРАВ ДОСТУПА
    conn.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY,
            user_role VARCHAR NOT NULL,
            cube_name VARCHAR NOT NULL,
            access_level VARCHAR DEFAULT 'READ',
            granted_by VARCHAR,
            granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_role, cube_name)
        )
    """)
    
    # ТАБЛИЦА КУБОВ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS olap_cubes (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            table_name VARCHAR NOT NULL,
            definition JSON,
            description TEXT,
            row_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner VARCHAR,
            is_public BOOLEAN DEFAULT FALSE
        )
    """)
    
    # ТАБЛИЦА СРЕЗОВ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS olap_slices (
            id INTEGER PRIMARY KEY,
            cube_name VARCHAR NOT NULL,
            slice_name VARCHAR NOT NULL,
            definition JSON,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner VARCHAR,
            UNIQUE(cube_name, slice_name)
        )
    """)
    
    # ТАБЛИЦА ИСТОРИИ ЗАПРОСОВ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS query_history (
            id INTEGER PRIMARY KEY,
            cube_name VARCHAR,
            query_text VARCHAR,
            execution_time FLOAT,
            rows_returned INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_name VARCHAR,
            user_id INTEGER,
            status VARCHAR DEFAULT 'SUCCESS',
            error_message VARCHAR
        )
    """)
    
    # ТАБЛИЦА ПАРТИЦИЙ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS table_partitions (
            id INTEGER PRIMARY KEY,
            table_name VARCHAR,
            partition_column VARCHAR,
            partition_value VARCHAR,
            row_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ТАБЛИЦА ДАШБОРДОВ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dashboards (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            cube_name VARCHAR NOT NULL,
            config JSON,
            layout JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner VARCHAR,
            is_public BOOLEAN DEFAULT FALSE
        )
    """)
    
    # ТАБЛИЦА ОТЧЁТОВ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_reports (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            cube_name VARCHAR NOT NULL,
            query_config JSON,
            schedule_type VARCHAR,
            schedule_config JSON,
            recipients JSON,
            last_run TIMESTAMP,
            next_run TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            owner VARCHAR,
            is_active BOOLEAN DEFAULT TRUE
        )
    """)
    
    # ТАБЛИЦА АУДИТА
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY,
            user_name VARCHAR,
            user_id INTEGER,
            action VARCHAR,
            details JSON,
            ip_address VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ТАБЛИЦА НАСТРОЕК СИСТЕМЫ
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key VARCHAR PRIMARY KEY,
            value JSON,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR
        )
    """)

def _create_default_users():
    """Создание демо-пользователей"""
    users = [
        (1, 'admin', 'admin123', 'ADMIN', 'admin@olap.local', 'System Administrator'),
        (2, 'test', 'test123', 'VIEWER', 'test@olap.local', 'Test User'),
        (3, 'analyst', 'analyst123', 'ANALYST', 'analyst@olap.local', 'Data Analyst'),
    ]
    
    for user_id, username, password, role, email, full_name in users:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            conn.execute("""
                INSERT INTO users (id, username, password_hash, role, email, full_name, is_active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
            """, [user_id, username, password_hash, role, email, full_name])
        except duckdb.ConstraintException:
            # Пользователь уже существует
            pass

def _create_default_permissions():
    """Создание прав доступа по умолчанию"""
    permissions = [
        (1, 'ADMIN', '*', 'ADMIN', 'system'),
        (2, 'ANALYST', '*', 'WRITE', 'system'),
        (3, 'VIEWER', '*', 'READ', 'system'),
    ]
    
    for perm_id, role, cube, level, granted_by in permissions:
        try:
            conn.execute("""
                INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by)
                VALUES (?, ?, ?, ?, ?)
            """, [perm_id, role, cube, level, granted_by])
        except duckdb.ConstraintException:
            pass

def _create_default_settings():
    """Настройки системы по умолчанию"""
    settings = [
        ('cache_ttl', '3600', 'system'),
        ('query_timeout', '30', 'system'),
        ('max_export_rows', '100000', 'system'),
        ('max_upload_size_mb', '50', 'system'),
        ('enable_audit', 'true', 'system'),
    ]
    
    for key, value, updated_by in settings:
        try:
            conn.execute("""
                INSERT INTO system_settings (key, value, updated_by)
                VALUES (?, ?, ?)
            """, [key, value, updated_by])
        except duckdb.ConstraintException:
            pass

def _ensure_schema_compatibility():
    """Проверка и обновление схемы БД при необходимости"""
    try:
        # Проверяем users
        columns = conn.execute("PRAGMA table_info(users)").fetchdf()
        if 'is_active' not in columns['name'].values:
            try:
                conn.execute("ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
            except:
                pass
        if 'full_name' not in columns['name'].values:
            try:
                conn.execute("ALTER TABLE users ADD COLUMN full_name VARCHAR")
            except:
                pass
        
        # Проверяем query_history
        qh_columns = conn.execute("PRAGMA table_info(query_history)").fetchdf()
        if 'user_id' not in qh_columns['name'].values:
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN user_id INTEGER")
            except:
                pass
        if 'error_message' not in qh_columns['name'].values:
            try:
                conn.execute("ALTER TABLE query_history ADD COLUMN error_message VARCHAR")
            except:
                pass
            
    except Exception as e:
        # Не прерываем работу при ошибке проверки схемы
        pass

def ensure_db_initialized():
    """Безопасная инициализация БД - не удаляет существующие данные"""
    if st.session_state.db_initialized:
        return True
    
    try:
        # Проверяем существование ключевой таблицы через duckdb_tables()
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()
        tables_exist = 'users' in tables['table_name'].values if not tables.empty else False
        
        if not tables_exist:
            # Первая установка - создаём всё с нуля
            _create_all_tables()
            _create_default_users()
            _create_default_permissions()
            _create_default_settings()
            log_audit("DB_INIT", {"status": "fresh_install", "version": APP_VERSION})
            st.sidebar.success("🎉 База данных создана!")
        else:
            # Существующая БД - проверяем совместимость
            _ensure_schema_compatibility()
            
            # Создаём демо-пользователей только если БД полностью пуста
            user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if user_count == 0:
                _create_default_users()
                _create_default_permissions()
                st.sidebar.info("👤 Созданы демо-пользователи")
            
            log_audit("DB_INIT", {"status": "existing_db_checked", "version": APP_VERSION})
        
        st.session_state.db_initialized = True
        return True
        
    except Exception as e:
        st.sidebar.error(f"❌ Ошибка инициализации БД: {e}")
        log_audit("DB_INIT_ERROR", {"error": str(e)[:200]})
        return False

# Вызываем инициализацию БД
ensure_db_initialized()

# ============================================
# 6. CSS СТИЛИ
# ============================================
st.markdown("""
<style>
    /* Основные стили */
    .main-header {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        padding: 20px;
        border-radius: 15px;
        color: white;
        margin-bottom: 20px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .olap-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 15px;
        transition: transform 0.2s;
    }
    
    .olap-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
    }
    
    .dimension-badge {
        background: #e3f2fd;
        color: #1976d2;
        padding: 5px 12px;
        border-radius: 20px;
        font-size: 0.9em;
        margin: 2px;
        display: inline-block;
        font-weight: 500;
    }
    
    .measure-badge {
        background: #fce4ec;
        color: #c2185b;
        padding: 5px 12px;
        border-radius: 20px;
        font-size: 0.9em;
        margin: 2px;
        display: inline-block;
        font-weight: 500;
    }
    
    .hierarchy-level {
        margin-left: 20px;
        padding: 5px;
        border-left: 2px solid #ddd;
    }
    
    .dashboard-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        margin: 10px 0;
    }
    
    /* Кнопки */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: bold;
        transition: all 0.3s;
        width: 100%;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }
    
    /* Вкладки */
    .stTabs [data-baseweb="tab-list"] {
        gap: 5px;
        background-color: #f5f5f5;
        border-radius: 10px;
        padding: 5px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: bold;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        color: white;
    }
    
    /* Таблицы */
    .dataframe th {
        background: #1e3c72 !important;
        color: white !important;
        padding: 12px !important;
        font-weight: 600;
    }
    
    .dataframe td {
        padding: 8px 12px !important;
    }
    
    /* Бейджи */
    .user-role-badge {
        background: #4caf50;
        color: white;
        padding: 3px 10px;
        border-radius: 12px;
        font-size: 0.8em;
        font-weight: 600;
    }
    
    .role-admin { background: #f44336; }
    .role-analyst { background: #2196f3; }
    .role-viewer { background: #4caf50; }
    
    /* Контейнер входа */
    .login-container {
        max-width: 400px;
        margin: 100px auto;
        padding: 40px;
        background: white;
        border-radius: 15px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
    }
    
    /* KPI карточки */
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 25px 15px;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }
    
    .kpi-value {
        font-size: 2.5em;
        font-weight: bold;
        line-height: 1.2;
    }
    
    .kpi-label {
        font-size: 0.9em;
        opacity: 0.9;
        margin-top: 5px;
    }
    
    /* Метрики */
    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        border: 1px solid #e9ecef;
    }
    
    .metric-value {
        font-size: 2em;
        font-weight: bold;
        color: #1e3c72;
    }
    
    .metric-label {
        color: #666;
        font-size: 0.9em;
        margin-top: 5px;
    }
    
    /* API эндпоинты */
    .api-endpoint {
        background: #f5f5f5;
        padding: 15px;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        margin: 5px 0;
        border-left: 4px solid #667eea;
    }
    
    /* Уведомления */
    .alert-success {
        background: #d4edda;
        color: #155724;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #28a745;
    }
    
    .alert-error {
        background: #f8d7da;
        color: #721c24;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #dc3545;
    }
    
    .alert-warning {
        background: #fff3cd;
        color: #856404;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #ffc107;
    }
    
    .alert-info {
        background: #d1ecf1;
        color: #0c5460;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #17a2b8;
    }
    
    /* Drill-down индикатор */
    .drill-indicator {
        background: #fff3e0;
        border-left: 4px solid #ff9800;
        padding: 15px;
        margin: 10px 0;
        border-radius: 8px;
    }
    
    /* Статус подключения */
    .connection-status {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 5px;
    }
    
    .status-online {
        background: #4caf50;
        box-shadow: 0 0 10px #4caf50;
    }
    
    .status-offline {
        background: #f44336;
    }
    
    /* Прогресс-бар */
    .progress-container {
        width: 100%;
        background: #e0e0e0;
        border-radius: 10px;
        margin: 10px 0;
    }
    
    .progress-bar {
        height: 20px;
        border-radius: 10px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        text-align: center;
        color: white;
        font-size: 12px;
        line-height: 20px;
    }
    
    /* Анимация загрузки */
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 1; }
        100% { opacity: 0.6; }
    }
    
    .loading {
        animation: pulse 1.5s infinite;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 7. КЭШИРОВАНИЕ ЗАПРОСОВ
# ============================================
class QueryCache:
    """Система кэширования запросов с ограничением размера и TTL"""
    
    def __init__(self, max_size: int = 100, default_ttl: int = 3600):
        self.cache: Dict[str, Dict] = {}
        self.cache_stats = {'hits': 0, 'misses': 0}
        self.max_size = max_size
        self.default_ttl = default_ttl
        
    def get_cache_key(self, query: str, params: tuple = ()) -> str:
        """Генерация детерминированного ключа кэша"""
        content = query + str(sorted(params) if params else '')
        return hashlib.sha256(content.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Получение из кэша с проверкой TTL"""
        if key not in self.cache:
            self.cache_stats['misses'] += 1
            return None
        
        entry = self.cache[key]
        # Проверяем срок жизни
        if (datetime.now() - entry['timestamp']).total_seconds() > entry['ttl']:
            del self.cache[key]
            self.cache_stats['misses'] += 1
            return None
            
        self.cache_stats['hits'] += 1
        return entry['data'].copy()
    
    def set(self, key: str, data: pd.DataFrame, ttl: int = None):
        """Сохранение в кэш с LRU-эвикцией"""
        if ttl is None:
            ttl = self.default_ttl
            
        # LRU: удаляем самый старый при переполнении
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]
        
        self.cache[key] = {
            'data': data.copy(),
            'timestamp': datetime.now(),
            'ttl': ttl,
            'size_bytes': data.memory_usage(deep=True).sum()
        }
        self._cleanup()
    
    def _cleanup(self):
        """Очистка устаревших записей"""
        now = datetime.now()
        expired = [k for k, v in self.cache.items() 
                  if (now - v['timestamp']).total_seconds() > v['ttl']]
        for key in expired:
            del self.cache[key]
    
    def clear(self):
        """Полная очистка кэша"""
        self.cache.clear()
        self.cache_stats = {'hits': 0, 'misses': 0}
    
    def get_stats(self) -> Dict:
        """Статистика кэша"""
        total = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_rate = self.cache_stats['hits'] / total if total > 0 else 0
        total_size = sum(e['size_bytes'] for e in self.cache.values())
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hits': self.cache_stats['hits'],
            'misses': self.cache_stats['misses'],
            'hit_rate': f"{hit_rate:.1%}",
            'memory_usage_mb': total_size / 1024 / 1024
        }

# ============================================
# 8. МОДЕЛЬ ДАННЫХ OLAP
# ============================================
class OLAPDimension:
    """Измерение OLAP с поддержкой иерархий и атрибутов"""
    def __init__(self, name: str, column: str, hierarchy: List[str] = None, 
                 description: str = "", data_type: str = "string"):
        self.name = name
        self.column = column
        self.hierarchy = hierarchy or []
        self.description = description
        self.data_type = data_type
        self.attributes: Dict[str, str] = {}
        
    def add_attribute(self, attr_name: str, column: str):
        self.attributes[attr_name] = column
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'column': self.column,
            'hierarchy': self.hierarchy,
            'description': self.description,
            'data_type': self.data_type,
            'attributes': self.attributes
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'OLAPDimension':
        dim = cls(
            data.get('name', ''),
            data['column'],
            data.get('hierarchy', []),
            data.get('description', ''),
            data.get('data_type', 'string')
        )
        dim.attributes = data.get('attributes', {})
        return dim
        
class OLAPMeasure:
    """Мера OLAP с поддержкой агрегаций и форматирования"""
    def __init__(self, name: str, column: str, default_agg: str = 'SUM', 
                 description: str = "", format: str = "", unit: str = "",
                 precision: int = 2):
        self.name = name
        self.column = column
        self.default_agg = default_agg.upper()
        self.description = description
        self.format = format
        self.unit = unit
        self.precision = precision
        self.allowed_aggs = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 
                            'COUNT_DISTINCT', 'MEDIAN', 'STDDEV', 'VARIANCE']
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'column': self.column,
            'default_agg': self.default_agg,
            'description': self.description,
            'format': self.format,
            'unit': self.unit,
            'precision': self.precision
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'OLAPMeasure':
        return cls(
            data.get('name', ''),
            data['column'],
            data.get('default_agg', 'SUM'),
            data.get('description', ''),
            data.get('format', ''),
            data.get('unit', ''),
            data.get('precision', 2)
        )
        
class OLAPCube:
    """OLAP Куб для многомерного анализа"""
    def __init__(self, name: str, table_name: str, description: str = ""):
        self.name = name
        self.table_name = table_name
        self.description = description
        self.dimensions: Dict[str, OLAPDimension] = {}
        self.measures: Dict[str, OLAPMeasure] = {}
        self.calculated_members: Dict[str, str] = {}
        self.partitions: List[Dict] = []
        self.indexes: List[str] = []
        self.metadata: Dict[str, Any] = {}
        
    def add_dimension(self, dim: OLAPDimension):
        self.dimensions[dim.name] = dim
        
    def add_measure(self, measure: OLAPMeasure):
        self.measures[measure.name] = measure
        
    def add_calculated_member(self, name: str, formula: str):
        self.calculated_members[name] = formula
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'table_name': self.table_name,
            'description': self.description,
            'dimensions': {n: d.to_dict() for n, d in self.dimensions.items()},
            'measures': {n: m.to_dict() for n, m in self.measures.items()},
            'calculated_members': self.calculated_members,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, name: str, table_name: str, data: Dict) -> 'OLAPCube':
        cube = cls(name, table_name, data.get('description', ''))
        
        for dim_name, dim_data in data.get('dimensions', {}).items():
            cube.add_dimension(OLAPDimension.from_dict(dim_data))
        
        for measure_name, measure_data in data.get('measures', {}).items():
            cube.add_measure(OLAPMeasure.from_dict(measure_data))
        
        cube.calculated_members = data.get('calculated_members', {})
        cube.metadata = data.get('metadata', {})
        
        return cube

# ============================================
# 9. OLAP МЕНЕДЖЕР
# ============================================
class OLAPManager:
    def __init__(self, conn):
        self.conn = conn
        self.cubes: Dict[str, OLAPCube] = {}
        self.query_cache = QueryCache()
        
    def _sanitize_identifier(self, name: str) -> str:
        """Создание безопасного SQL-идентификатора"""
        return name.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
        
    def create_cube_from_dataframe(self, name: str, df: pd.DataFrame, 
                                  description: str = "",
                                  auto_detect: bool = True,
                                  partition_by: str = None) -> Optional[OLAPCube]:
        """Создание куба из DataFrame с оптимизацией"""
        try:
            # Безопасное имя таблицы
            table_name = f"cube_{self._sanitize_identifier(name)}"
            
            # Регистрация и создание таблицы
            self.conn.register('temp_df', df)
            self.conn.execute(f"CREATE OR REPLACE TABLE \"{table_name}\" AS SELECT * FROM temp_df")
            
            # Создание индексов для частых колонок
            for col in df.columns:
                if df[col].nunique() < 1000 and df[col].nunique() > 1:
                    try:
                        idx_name = f"idx_{table_name}_{self._sanitize_identifier(col)}"
                        self.conn.execute(f"CREATE INDEX IF NOT EXISTS \"{idx_name}\" ON \"{table_name}\"(\"{col}\")")
                    except:
                        pass  # Индекс не критичен
            
            cube = OLAPCube(name, table_name, description)
            cube.metadata = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'created_by': st.session_state.get('username', 'system'),
                'created_at': datetime.now().isoformat()
            }
            
            if auto_detect:
                for col in df.columns:
                    # Определение мер (числовые колонки)
                    if pd.api.types.is_numeric_dtype(df[col]):
                        # Умный выбор агрегации
                        col_lower = col.lower()
                        if any(kw in col_lower for kw in ['price', 'rate', 'avg', 'percent']):
                            agg = 'AVG'
                        elif any(kw in col_lower for kw in ['count', 'qty', 'quantity']):
                            agg = 'SUM'
                        else:
                            agg = 'SUM'
                        measure = OLAPMeasure(col, col, agg, f"Агрегация: {col}")
                        cube.add_measure(measure)
                    
                    # Определение временных измерений
                    elif pd.api.types.is_datetime64_any_dtype(df[col]):
                        dim = OLAPDimension(col, col, ['Year', 'Quarter', 'Month', 'Day'], 
                                           f"Дата: {col}", data_type='datetime')
                        cube.add_dimension(dim)
                    
                    # Определение категориальных измерений
                    else:
                        unique_count = df[col].nunique()
                        if 1 < unique_count < 500:  # Разумный предел для измерения
                            dim = OLAPDimension(col, col, 
                                               description=f"Категория: {col} ({unique_count} значений)",
                                               data_type='categorical')
                            cube.add_dimension(dim)
            
            self.cubes[name] = cube
            self._save_cube_metadata(cube)
            
            log_audit("CREATE_CUBE", {
                "cube": name, 
                "rows": len(df), 
                "columns": len(df.columns),
                "dimensions": len(cube.dimensions),
                "measures": len(cube.measures)
            })
            
            return cube
            
        except Exception as e:
            st.error(f"❌ Ошибка создания куба: {e}")
            log_audit("CREATE_CUBE_ERROR", {"cube": name, "error": str(e)[:200]})
            return None
    
    def _save_cube_metadata(self, cube: OLAPCube):
        """Сохранение метаданных куба в БД"""
        definition = cube.to_dict()
        current_user = st.session_state.get('username', 'admin')
        
        exists = self.conn.execute(
            "SELECT COUNT(*) FROM olap_cubes WHERE name = ?", [cube.name]
        ).fetchone()[0]
        
        if exists > 0:
            self.conn.execute("""
                UPDATE olap_cubes 
                SET table_name = ?, definition = ?, updated_at = CURRENT_TIMESTAMP, 
                    owner = ?, description = ?, row_count = ?
                WHERE name = ?
            """, [cube.table_name, json.dumps(definition, default=str), current_user, 
                  cube.description, cube.metadata.get('row_count', 0), cube.name])
        else:
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM olap_cubes"
            ).fetchone()[0]
            self.conn.execute("""
                INSERT INTO olap_cubes (id, name, table_name, definition, description, row_count, owner)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [max_id + 1, cube.name, cube.table_name, json.dumps(definition, default=str), 
                  cube.description, cube.metadata.get('row_count', 0), current_user])
    
    def load_cube(self, name: str) -> Optional[OLAPCube]:
        """Загрузка куба из БД"""
        try:
            result = self.conn.execute(
                "SELECT definition, table_name, description FROM olap_cubes WHERE name = ?",
                [name]
            ).fetchone()
            
            if result:
                definition = json.loads(result[0])
                cube = OLAPCube.from_dict(name, result[1], definition)
                cube.description = result[2] or ""
                
                self.cubes[name] = cube
                log_audit("LOAD_CUBE", {"cube": name})
                return cube
                
        except json.JSONDecodeError as e:
            st.error(f"❌ Ошибка парсинга метаданных куба: {e}")
        except Exception as e:
            st.error(f"❌ Ошибка загрузки куба: {e}")
            log_audit("LOAD_CUBE_ERROR", {"cube": name, "error": str(e)[:200]})
        return None
    
    def slice_dice(self, cube_name: str, rows: List[str], cols: List[str], 
                   measures: List[str], filters: Dict = None) -> pd.DataFrame:
        """Операция slice and dice с обработкой ошибок"""
        if cube_name not in self.cubes:
            st.error(f"❌ Куб '{cube_name}' не загружен")
            return pd.DataFrame()
        
        cube = self.cubes[cube_name]
        dimensions = list(set(rows + cols))
        
        # Фильтрация валидных мер
        valid_measures = [m for m in measures if m in cube.measures]
        if not valid_measures:
            st.warning("⚠️ Нет доступных мер для отображения")
            return pd.DataFrame()
        
        measures_with_agg = [(m, cube.measures[m].default_agg) for m in valid_measures]
        df = self.query_cube(cube_name, dimensions, measures_with_agg, filters)
        
        if df.empty:
            return df
        
        try:
            # Построение сводной таблицы
            if rows and cols:
                pivot = df.pivot_table(
                    index=rows, columns=cols, values=valid_measures, 
                    aggfunc='sum', fill_value=0, dropna=False
                )
                # Если одна мера, pivot_table может вернуть Series вместо DataFrame
                if isinstance(pivot, pd.Series):
                    return pivot.to_frame()
                return pivot
            elif rows:
                return df.groupby(rows, dropna=False)[valid_measures].sum().reset_index()
            elif cols:
                return df.groupby(cols, dropna=False)[valid_measures].sum().reset_index()
            else:
                return df[valid_measures].sum().to_frame().T
        except Exception as e:
            st.error(f"❌ Ошибка построения сводной таблицы: {e}")
            return df
    
    def query_cube(self, cube_name: str, 
                   dimensions: List[str], 
                   measures: List[Tuple[str, str]],
                   filters: Dict[str, Any] = None,
                   top_n: int = None,
                   order_by: List[Tuple[str, str]] = None,
                   use_cache: bool = True,
                   timeout: int = 30) -> pd.DataFrame:
        """Оптимизированное выполнение запроса с кэшированием и таймаутом"""
        
        start_time = datetime.now()
        
        # Генерация надёжного ключа кэша
        cache_data = {
            'cube': cube_name,
            'dims': sorted(dimensions),
            'meas': sorted(measures),
            'filt': json.dumps(filters, sort_keys=True, default=str) if filters else None,
            'top': top_n,
            'order': sorted(order_by) if order_by else None
        }
        cache_key = hashlib.sha256(
            json.dumps(cache_data, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        # Проверка кэша
        if use_cache:
            cached_result = self.query_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        if cube_name not in self.cubes:
            return pd.DataFrame()
        
        cube = self.cubes[cube_name]
        table_name = cube.table_name
        
        # Построение SELECT части
        select_parts = []
        group_by_parts = []
        
        for dim_name in dimensions:
            if dim_name in cube.dimensions:
                dim = cube.dimensions[dim_name]
                safe_col = dim.column.replace('"', '""')
                select_parts.append(f'"{safe_col}" as "{dim_name}"')
                group_by_parts.append(f'"{safe_col}"')
        
        for measure_name, agg_func in measures:
            if measure_name in cube.measures:
                measure = cube.measures[measure_name]
                agg_func = (agg_func or measure.default_agg).upper()
                safe_col = measure.column.replace('"', '""')
                
                if agg_func == 'COUNT_DISTINCT':
                    select_parts.append(f'COUNT(DISTINCT "{safe_col}") as "{measure_name}"')
                elif agg_func == 'MEDIAN':
                    select_parts.append(f'MEDIAN("{safe_col}") as "{measure_name}"')
                elif agg_func == 'STDDEV':
                    select_parts.append(f'STDDEV("{safe_col}") as "{measure_name}"')
                elif agg_func == 'VARIANCE':
                    select_parts.append(f'VARIANCE("{safe_col}") as "{measure_name}"')
                else:
                    select_parts.append(f'{agg_func}("{safe_col}") as "{measure_name}"')
        
        if not select_parts:
            return pd.DataFrame()
        
        # Построение полного запроса
        query = f"SELECT {', '.join(select_parts)} FROM \"{table_name}\""
        
        # WHERE clause с параметризацией
        if filters:
            where_conditions = []
            for col, value in filters.items():
                safe_col = col.replace('"', '""')
                if isinstance(value, list) and value:
                    # IN clause
                    values_str = ', '.join([f"'{str(v).replace(\"'\", \"''\")}'" for v in value])
                    where_conditions.append(f'"{safe_col}" IN ({values_str})')
                elif isinstance(value, dict):
                    # Range filter
                    if 'min' in value and value['min'] is not None:
                        where_conditions.append(f'"{safe_col}" >= {value["min"]}')
                    if 'max' in value and value['max'] is not None:
                        where_conditions.append(f'"{safe_col}" <= {value["max"]}')
                elif value is not None:
                    if isinstance(value, str):
                        where_conditions.append(f'"{safe_col}" = \'{value.replace("\'", "\'\'")}\')')
                    else:
                        where_conditions.append(f'"{safe_col}" = {value}')
            
            if where_conditions:
                query += f" WHERE {' AND '.join(where_conditions)}"
        
        if group_by_parts:
            query += f" GROUP BY {', '.join(group_by_parts)}"
        
        if order_by:
            order_parts = [f'"{col.replace("\"", "\"\"")}" {direction.upper()}' 
                          for col, direction in order_by]
            query += f" ORDER BY {', '.join(order_parts)}"
        
        if top_n and top_n > 0:
            query += f" LIMIT {int(top_n)}"
        
        try:
            # Установка таймаута
            self.conn.execute(f"SET query_timeout = '{timeout}s'")
            
            result = self.conn.execute(query).fetchdf()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            self._log_query(cube_name, query, execution_time, len(result), 'SUCCESS')
            
            if use_cache and not result.empty:
                self.query_cache.set(cache_key, result)
            
            return result
            
        except duckdb.TimeoutException:
            self._log_query(cube_name, query, 
                           (datetime.now() - start_time).total_seconds(), 
                           0, 'TIMEOUT', f'Query exceeded {timeout}s limit')
            st.error(f"⏱️ Запрос превысил лимит времени ({timeout}с). Добавьте фильтры или уменьшите данные.")
            return pd.DataFrame()
            
        except duckdb.Error as e:
            error_msg = str(e)
            self._log_query(cube_name, query, 
                           (datetime.now() - start_time).total_seconds(), 
                           0, 'DB_ERROR', error_msg[:200])
            
            # Дружелюбные сообщения об ошибках
            if 'no such column' in error_msg.lower():
                st.error("❌ В запросе указана несуществующая колонка")
            elif 'permission denied' in error_msg.lower():
                st.error("🔒 Недостаточно прав для выполнения запроса")
            elif 'out of memory' in error_msg.lower():
                st.error("💾 Не хватает памяти. Попробуйте добавить фильтры.")
            else:
                st.error(f"❌ Ошибка выполнения запроса: {error_msg[:150]}...")
            return pd.DataFrame()
            
        except Exception as e:
            self._log_query(cube_name, query, 
                           (datetime.now() - start_time).total_seconds(), 
                           0, 'ERROR', f"{type(e).__name__}: {str(e)[:100]}")
            st.error(f"⚠️ Неожиданная ошибка: {type(e).__name__}")
            return pd.DataFrame()
    
    def _log_query(self, cube_name: str, query: str, execution_time: float, 
                   rows: int, status: str, error_message: str = None):
        """Логирование запроса с безопасной обработкой"""
        try:
            current_user = st.session_state.get('username', 'anonymous')
            user_id = st.session_state.get('user_id')
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM query_history"
            ).fetchone()[0]
            
            # DuckDB не поддерживает None в INSERT для некоторых типов, лучше передать пустую строку или null через параметры
            params = [max_id + 1, cube_name, query[:1000], execution_time, rows, 
                      current_user, user_id, status, error_message]
            self.conn.execute("""
                INSERT INTO query_history (id, cube_name, query_text, execution_time, 
                                          rows_returned, user_name, user_id, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, params)
        except Exception as e:
            # Не прерываем работу при ошибке логирования
            # print(f"Log error: {e}")
            pass
    
    def get_query_performance_stats(self) -> pd.DataFrame:
        """Статистика производительности запросов"""
        try:
            return self.conn.execute("""
                SELECT 
                    cube_name,
                    COUNT(*) as query_count,
                    ROUND(AVG(execution_time), 3) as avg_time,
                    ROUND(MAX(execution_time), 3) as max_time,
                    AVG(rows_returned) as avg_rows,
                    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_count
                FROM query_history
                WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
                GROUP BY cube_name
                ORDER BY avg_time DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def create_materialized_view(self, cube_name: str, view_name: str, 
                                 dimensions: List[str], measures: List[str]):
        """Создание материализованного представления"""
        cube = self.cubes[cube_name]
        measures_with_agg = [(m, cube.measures[m].default_agg) for m in measures]
        df = self.query_cube(cube_name, dimensions, measures_with_agg)
        view_table = f"mv_{self._sanitize_identifier(view_name)}"
        
        self.conn.register('mv_df', df)
        self.conn.execute(f"CREATE OR REPLACE TABLE \"{view_table}\" AS SELECT * FROM mv_df")
        
        log_audit("CREATE_MVIEW", {"cube": cube_name, "view": view_table})
        return view_table
    
    def get_cubes_list(self) -> pd.DataFrame:
        """Список всех кубов"""
        try:
            return self.conn.execute("""
                SELECT name, description, row_count, created_at, updated_at, owner, is_public
                FROM olap_cubes
                ORDER BY updated_at DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def delete_cube(self, name: str) -> bool:
        """Удаление куба"""
        try:
            cube = self.cubes.get(name)
            if cube:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS \"{cube.table_name}\"")
                except:
                    pass
            self.conn.execute("DELETE FROM olap_cubes WHERE name = ?", [name])
            self.conn.execute("DELETE FROM olap_slices WHERE cube_name = ?", [name])
            self.conn.execute("DELETE FROM dashboards WHERE cube_name = ?", [name])
            if name in self.cubes:
                del self.cubes[name]
            
            log_audit("DELETE_CUBE", {"cube": name})
            return True
        except Exception as e:
            log_audit("DELETE_CUBE_ERROR", {"cube": name, "error": str(e)[:200]})
            return False
    
    def get_table_info(self, cube_name: str) -> Dict:
        """Информация о таблице куба"""
        try:
            cube = self.cubes.get(cube_name)
            if not cube:
                return {}
            
            row_count = self.conn.execute(
                f"SELECT COUNT(*) FROM \"{cube.table_name}\""
            ).fetchone()[0]
            col_count = len(self.conn.execute(
                f"PRAGMA table_info('{cube.table_name}')"
            ).fetchdf())
            
            return {
                'table_name': cube.table_name,
                'row_count': row_count,
                'column_count': col_count,
                'dimension_count': len(cube.dimensions),
                'measure_count': len(cube.measures)
            }
        except:
            return {}

# ============================================
# 10. СИСТЕМА ПОЛЬЗОВАТЕЛЕЙ
# ============================================
class UserManager:
    def __init__(self, conn):
        self.conn = conn
    
    def _check_login_attempts(self, username: str) -> bool:
        """Проверка на превышение попыток входа (защита от брутфорса)"""
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) FROM audit_log 
                WHERE action = 'LOGIN_FAILED' 
                AND user_name = ?
                AND timestamp > CURRENT_TIMESTAMP - INTERVAL '15 minutes'
            """, [username]).fetchone()
            
            if result and result[0] >= 5:
                return False
            return True
        except:
            return True  # Fail-open при ошибке проверки
    
    def authenticate(self, username: str, password: str) -> bool:
        """Безопасная аутентификация с детальными сообщениями"""
        if not username or not password:
            st.warning("⚠️ Введите логин и пароль")
            return False
        
        # Проверка на брутфорс
        if not self._check_login_attempts(username):
            st.error("🔒 Слишком много попыток входа. Попробуйте через 15 минут.")
            return False
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        try:
            # Получаем пользователя
            user = self.conn.execute("""
                SELECT id, username, role, password_hash, is_active, email, full_name
                FROM users 
                WHERE LOWER(username) = LOWER(?)
            """, [username.strip()]).fetchone()
            
            if not user:
                st.error(f"❌ Пользователь '{username}' не найден")
                log_audit("LOGIN_FAILED", {"username": username, "reason": "user_not_found"})
                return False
            
            user_id, db_username, role, stored_hash, is_active, email, full_name = user
            
            # Проверка пароля
            if stored_hash != password_hash:
                st.error("❌ Неверный пароль")
                log_audit("LOGIN_FAILED", {"username": username, "reason": "wrong_password"})
                return False
            
            # Проверка статуса аккаунта
            if is_active is False:
                st.error("🔒 Учётная запись заблокирована. Обратитесь к администратору.")
                log_audit("LOGIN_DENIED", {"username": username, "reason": "account_disabled"})
                return False
            
            # === УСПЕШНЫЙ ВХОД ===
            st.session_state.authenticated = True
            st.session_state.username = db_username
            st.session_state.role = role
            st.session_state.user_id = user_id
            st.session_state.user_email = email
            st.session_state.user_fullname = full_name
            st.session_state.last_login = datetime.now()
            
            # Обновляем last_login
            self.conn.execute(
                "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?",
                [user_id]
            )
            
            log_audit("LOGIN_SUCCESS", {
                "user_id": user_id,
                "username": db_username, 
                "role": role
            })
            
            return True
            
        except duckdb.Error as e:
            st.error(f"🔌 Ошибка базы данных: {e}")
            log_audit("LOGIN_DB_ERROR", {"username": username, "error": str(e)[:200]})
            return False
        except Exception as e:
            st.error(f"⚠️ Системная ошибка: {type(e).__name__}")
            log_audit("LOGIN_EXCEPTION", {"username": username, "error": f"{type(e).__name__}: {str(e)[:100]}"})
            return False
    
    def create_user(self, username: str, password: str, role: str = 'VIEWER', 
                    email: str = "", full_name: str = "") -> bool:
        """Создание нового пользователя"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        if not username or not password:
            return False
            
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            # Проверка уникальности
            exists = self.conn.execute(
                "SELECT COUNT(*) FROM users WHERE LOWER(username) = LOWER(?)", 
                [username]
            ).fetchone()[0]
            if exists > 0:
                return False
            
            max_id = self.conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM users"
            ).fetchone()[0]
            self.conn.execute("""
                INSERT INTO users (id, username, password_hash, role, email, full_name, is_active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
            """, [max_id + 1, username, password_hash, role, email, full_name])
            
            log_audit("CREATE_USER", {"username": username, "role": role})
            return True
        except:
            return False
    
    def update_user(self, username: str, role: str = None, email: str = None,
                    full_name: str = None, password: str = None, 
                    is_active: bool = None) -> bool:
        """Обновление пользователя"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        try:
            updates = []
            params = []
            if role:
                updates.append("role = ?")
                params.append(role)
            if email:
                updates.append("email = ?")
                params.append(email)
            if full_name:
                updates.append("full_name = ?")
                params.append(full_name)
            if password:
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                updates.append("password_hash = ?")
                params.append(password_hash)
            if is_active is not None:
                updates.append("is_active = ?")
                params.append(is_active)
            
            if updates:
                params.append(username)
                self.conn.execute(
                    f"UPDATE users SET {', '.join(updates)} WHERE username = ?", 
                    params
                )
            
            log_audit("UPDATE_USER", {"username": username})
            return True
        except:
            return False
    
    def delete_user(self, username: str) -> bool:
        """Удаление пользователя"""
        if st.session_state.get('role') != 'ADMIN' or username.lower() == 'admin':
            return False
        
        try:
            self.conn.execute("DELETE FROM users WHERE username = ?", [username])
            log_audit("DELETE_USER", {"username": username})
            return True
        except:
            return False
    
    def check_permission(self, cube_name: str, required_level: str = 'READ') -> bool:
        """Проверка прав доступа"""
        if 'username' not in st.session_state:
            return False
        
        role = st.session_state.get('role', 'VIEWER')
        if role == 'ADMIN':
            return True
        
        try:
            result = self.conn.execute("""
                SELECT access_level FROM permissions 
                WHERE user_role = ? AND (cube_name = ? OR cube_name = '*')
                ORDER BY CASE WHEN cube_name = '*' THEN 1 ELSE 0 END
                LIMIT 1
            """, [role, cube_name]).fetchone()
            
            if not result:
                return False
            
            levels = {'READ': 1, 'WRITE': 2, 'ADMIN': 3}
            return levels.get(result[0], 0) >= levels.get(required_level, 1)
        except:
            return False
    
    def get_users_list(self) -> pd.DataFrame:
        """Список пользователей"""
        try:
            return self.conn.execute("""
                SELECT username, role, email, full_name, created_at, last_login, is_active
                FROM users 
                ORDER BY created_at DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def grant_permission(self, role: str, cube_name: str, access_level: str) -> bool:
        """Назначение прав"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        try:
            exists = self.conn.execute(
                "SELECT COUNT(*) FROM permissions WHERE user_role = ? AND cube_name = ?",
                [role, cube_name]
            ).fetchone()[0]
            
            granted_by = st.session_state.get('username', 'system')
            
            if exists > 0:
                self.conn.execute("""
                    UPDATE permissions 
                    SET access_level = ?, granted_by = ?, granted_at = CURRENT_TIMESTAMP 
                    WHERE user_role = ? AND cube_name = ?
                """, [access_level, granted_by, role, cube_name])
            else:
                max_id = self.conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM permissions"
                ).fetchone()[0]
                self.conn.execute("""
                    INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by)
                    VALUES (?, ?, ?, ?, ?)
                """, [max_id + 1, role, cube_name, access_level, granted_by])
            
            log_audit("GRANT_PERMISSION", {"role": role, "cube": cube_name, "level": access_level})
            return True
        except:
            return False
    
    def revoke_permission(self, role: str, cube_name: str) -> bool:
        """Отзыв прав"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        try:
            self.conn.execute(
                "DELETE FROM permissions WHERE user_role = ? AND cube_name = ?", 
                [role, cube_name]
            )
            log_audit("REVOKE_PERMISSION", {"role": role, "cube": cube_name})
            return True
        except:
            return False
    
    def get_permissions_list(self) -> pd.DataFrame:
        """Список всех прав"""
        try:
            return self.conn.execute("""
                SELECT user_role, cube_name, access_level, granted_by, granted_at
                FROM permissions
                ORDER BY user_role, cube_name
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def get_user_role_class(self, role: str) -> str:
        """CSS класс для роли"""
        return {
            'ADMIN': 'role-admin',
            'ANALYST': 'role-analyst',
            'VIEWER': 'role-viewer'
        }.get(role, '')

# ============================================
# 11. ДАШБОРДЫ И ВИЗУАЛИЗАЦИИ
# ============================================
class DashboardManager:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
        self.conn = olap_manager.conn
    
    def create_treemap(self, cube_name: str, dimension: str, measure: str, top_n: int = 20):
        """Treemap визуализация"""
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            top_n=top_n, order_by=[(measure, 'DESC')]
        )
        if df.empty:
            return None
        fig = px.treemap(
            df, path=[dimension], values=measure,
            title=f"Распределение {measure} по {dimension}",
            color=measure, color_continuous_scale='RdBu'
        )
        fig.update_layout(height=500)
        return fig
    
    def create_bar_chart(self, cube_name: str, dimension: str, measure: str, 
                         top_n: int = 10, horizontal: bool = False):
        """Столбчатая диаграмма"""
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            top_n=top_n, order_by=[(measure, 'DESC')]
        )
        if df.empty:
            return None
        if horizontal:
            fig = px.bar(df, y=dimension, x=measure, orientation='h',
                        title=f"{measure} по {dimension}",
                        color=measure, color_continuous_scale='Blues')
        else:
            fig = px.bar(df, x=dimension, y=measure,
                        title=f"{measure} по {dimension}",
                        color=measure, color_continuous_scale='Blues')
        fig.update_layout(height=500)
        return fig
    
    def create_line_chart(self, cube_name: str, date_dim: str, measure: str):
        """Линейный график"""
        df = self.olap_manager.query_cube(
            cube_name, [date_dim], [(measure, 'SUM')],
            order_by=[(date_dim, 'ASC')]
        )
        if df.empty:
            return None
        fig = px.line(df, x=date_dim, y=measure, title=f"Динамика {measure}", markers=True)
        fig.update_layout(height=500)
        return fig
    
    def create_pie_chart(self, cube_name: str, dimension: str, measure: str, top_n: int = 10):
        """Круговая диаграмма"""
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            top_n=top_n, order_by=[(measure, 'DESC')]
        )
        if df.empty:
            return None
        fig = px.pie(df, values=measure, names=dimension, 
                    title=f"Доля {measure} по {dimension}")
        fig.update_layout(height=500)
        return fig
    
    def create_heatmap(self, cube_name: str, row_dim: str, col_dim: str, measure: str):
        """Тепловая карта"""
        pivot_df = self.olap_manager.slice_dice(cube_name, [row_dim], [col_dim], [measure])
        if pivot_df.empty:
            return None
        fig = px.imshow(pivot_df, title=f"Heatmap: {measure}",
                       color_continuous_scale='RdBu_r', aspect='auto')
        fig.update_layout(height=500)
        return fig
    
    def create_scatter_plot(self, cube_name: str, x_measure: str, y_measure: str, 
                           color_dim: str = None, size_measure: str = None):
        """Диаграмма рассеяния"""
        dims = [color_dim] if color_dim else []
        measures = [(x_measure, 'SUM'), (y_measure, 'SUM')]
        if size_measure:
            measures.append((size_measure, 'SUM'))
        df = self.olap_manager.query_cube(cube_name, dims, measures)
        if df.empty:
            return None
        kwargs = {'x': x_measure, 'y': y_measure, 
                 'title': f"Корреляция {x_measure} и {y_measure}"}
        if color_dim and color_dim in df.columns:
            kwargs['color'] = color_dim
        if size_measure and size_measure in df.columns:
            kwargs['size'] = size_measure
        fig = px.scatter(df, **kwargs)
        fig.update_layout(height=500)
        return fig
    
    def create_waterfall(self, cube_name: str, dimension: str, measure: str):
        """Waterfall диаграмма"""
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            order_by=[(dimension, 'ASC')]
        )
        if df.empty:
            return None
        fig = go.Figure(go.Waterfall(
            name="Изменения", orientation="v",
            measure=["relative"] * len(df),
            x=df[dimension], y=df[measure],
            text=[f"{v:,.0f}" for v in df[measure]],
            textposition="outside",
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        fig.update_layout(title=f"Waterfall анализ {measure}", height=500)
        return fig
    
    def create_box_plot(self, cube_name: str, dimension: str, measure: str):
        """Box Plot"""
        df = self.olap_manager.query_cube(cube_name, [dimension], [(measure, 'SUM')])
        if df.empty:
            return None
        fig = px.box(df, x=dimension, y=measure, 
                    title=f"Box Plot: {measure} по {dimension}")
        fig.update_layout(height=500)
        return fig
    
    def create_histogram(self, cube_name: str, measure: str, bins: int = 20):
        """Гистограмма"""
        df = self.olap_manager.query_cube(cube_name, [], [(measure, 'SUM')])
        if df.empty:
            return None
        fig = px.histogram(df, x=measure, nbins=bins, title=f"Гистограмма: {measure}")
        fig.update_layout(height=500)
        return fig
    
    def create_area_chart(self, cube_name: str, date_dim: str, measure: str):
        """Диаграмма с областями"""
        df = self.olap_manager.query_cube(
            cube_name, [date_dim], [(measure, 'SUM')],
            order_by=[(date_dim, 'ASC')]
        )
        if df.empty:
            return None
        fig = px.area(df, x=date_dim, y=measure, title=f"Динамика {measure} (области)")
        fig.update_layout(height=500)
        return fig
    
    def create_kpi_cards(self, cube_name: str, measures: List[str]) -> Dict:
        """KPI карточки"""
        kpis = {}
        cube = self.olap_manager.cubes.get(cube_name)
        for measure in measures:
            if cube and measure in cube.measures:
                measure_obj = cube.measures[measure]
                df = self.olap_manager.query_cube(
                    cube_name, [], [(measure, measure_obj.default_agg)]
                )
                current = df[measure].iloc[0] if not df.empty else 0
                kpis[measure] = {
                    'current': current,
                    'unit': measure_obj.unit or '',
                    'format': measure_obj.format or '',
                    'change': 0, 'change_pct': 0
                }
        return kpis
    
    def save_dashboard(self, name: str, cube_name: str, config: Dict, layout: Dict = None) -> bool:
        """Сохранение дашборда"""
        try:
            exists = self.conn.execute(
                "SELECT COUNT(*) FROM dashboards WHERE name = ? AND cube_name = ?",
                [name, cube_name]
            ).fetchone()[0]
            current_user = st.session_state.get('username', 'admin')
            if exists > 0:
                self.conn.execute("""
                    UPDATE dashboards 
                    SET config = ?, layout = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE name = ? AND cube_name = ?
                """, [json.dumps(config, default=str), json.dumps(layout or {}, default=str), name, cube_name])
            else:
                max_id = self.conn.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM dashboards"
                ).fetchone()[0]
                self.conn.execute("""
                    INSERT INTO dashboards (id, name, cube_name, config, layout, owner)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [max_id + 1, name, cube_name, json.dumps(config, default=str), 
                      json.dumps(layout or {}, default=str), current_user])
            log_audit("SAVE_DASHBOARD", {"name": name, "cube": cube_name})
            return True
        except:
            return False
    
    def load_dashboards(self, cube_name: str = None) -> pd.DataFrame:
        """Загрузка дашбордов"""
        try:
            if cube_name:
                return self.conn.execute("""
                    SELECT id, name, cube_name, config, layout, created_at, updated_at, owner
                    FROM dashboards WHERE cube_name = ? ORDER BY updated_at DESC
                """, [cube_name]).fetchdf()
            else:
                return self.conn.execute("""
                    SELECT id, name, cube_name, config, layout, created_at, updated_at, owner
                    FROM dashboards ORDER BY updated_at DESC
                """).fetchdf()
        except:
            return pd.DataFrame()
    
    def delete_dashboard(self, dashboard_id: int) -> bool:
        """Удаление дашборда"""
        try:
            self.conn.execute("DELETE FROM dashboards WHERE id = ?", [dashboard_id])
            log_audit("DELETE_DASHBOARD", {"id": dashboard_id})
            return True
        except:
            return False
    
    def export_dashboard_to_html(self, figures: List[go.Figure], title: str = "OLAP Dashboard") -> str:
        """Экспорт дашборда в HTML"""
        html = f"""<!DOCTYPE html><html><head><title>{title}</title>
<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
<style>body{{font-family:'Segoe UI',Arial,sans-serif;background:#f5f5f5;margin:0;padding:20px}}
.dashboard-container{{max-width:1400px;margin:0 auto}}
.chart{{background:white;border-radius:10px;padding:20px;margin:20px 0;box-shadow:0 2px 10px rgba(0,0,0,0.1)}}
h1{{color:#1e3c72;text-align:center;margin-bottom:30px}}
.footer{{text-align:center;color:#666;margin-top:30px;padding:20px}}</style></head><body>
<div class='dashboard-container'><h1>🎲 {title}</h1>"""
        for fig in figures:
            html += f"<div class='chart'>{fig.to_html(include_plotlyjs=False)}</div>"
        html += f"<div class='footer'><p>OLAP Analytics Pro v{APP_VERSION} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p></div></div></body></html>"
        return html

# ============================================
# 12. API ДЛЯ ВНЕШНИХ СИСТЕМ
# ============================================
class OLAPAPI:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
    
    def execute_mdx_query(self, cube_name: str, mdx_query: str) -> Dict:
        """Выполнение MDX-подобного запроса"""
        result = {'cube': cube_name, 'query': mdx_query, 'result': None, 'error': None}
        try:
            # Простой парсер MDX (базовая поддержка)
            if 'SELECT' in mdx_query.upper():
                # Парсинг мер из секции ON COLUMNS
                measures_start = mdx_query.upper().find('SELECT') + 6
                measures_end = mdx_query.upper().find('ON COLUMNS')
                if measures_end == -1:
                    measures_end = mdx_query.upper().find('ON ROWS')
                
                if measures_end != -1:
                    measures_str = mdx_query[measures_start:measures_end].strip()
                    # Удаление фигурных скобок и лишних символов
                    measures_str = measures_str.replace('{', '').replace('}', '').replace('[Measures].', '')
                    measures = [m.strip(' []') for m in measures_str.split(',') if m.strip()]
                else:
                    measures = []

                # Парсинг измерений из секции ON ROWS
                rows_start = mdx_query.upper().find('ON ROWS')
                if rows_start > 0:
                    # Ищем FROM после ON ROWS
                    from_pos = mdx_query.upper().find('FROM', rows_start)
                    if from_pos > 0:
                        dimensions_str = mdx_query[rows_start + 7:from_pos].strip()
                    else:
                        dimensions_str = mdx_query[rows_start + 7:].strip()
                    
                    dimensions_str = dimensions_str.replace('{', '').replace('}', '').replace('[Dimension].', '')
                    dimensions = [d.strip(' []') for d in dimensions_str.split(',') if d.strip()]
                else:
                    dimensions = []
                
                measures_with_agg = [(m, 'SUM') for m in measures if m]
                df = self.olap_manager.query_cube(cube_name, dimensions, measures_with_agg)
                result['result'] = df.to_dict('records')
        except Exception as e:
            result['error'] = str(e)
        return result
    
    def execute_query(self, cube_name: str, query_config: Dict) -> Dict:
        """Выполнение запроса через API"""
        try:
            dimensions = query_config.get('dimensions', [])
            measures = [(m, query_config.get('aggregations', {}).get(m, 'SUM')) 
                       for m in query_config.get('measures', [])]
            filters = query_config.get('filters', {})
            top_n = query_config.get('top_n')
            order_by = query_config.get('order_by', [])
            
            df = self.olap_manager.query_cube(cube_name, dimensions, measures, filters, top_n, order_by)
            return {
                'success': True, 'data': df.to_dict('records'),
                'row_count': len(df), 'columns': df.columns.tolist()
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def export_data(self, cube_name: str, format: str = 'csv', query_config: Dict = None) -> bytes:
        """Экспорт данных"""
        if query_config:
            dimensions = query_config.get('dimensions', [])
            measures = [(m, 'SUM') for m in query_config.get('measures', [])]
            filters = query_config.get('filters', {})
            df = self.olap_manager.query_cube(cube_name, dimensions, measures, filters)
        else:
            cube = self.olap_manager.cubes.get(cube_name)
            if not cube:
                return b''
            dimensions = list(cube.dimensions.keys())
            measures = [(m, 'SUM') for m in cube.measures.keys()]
            df = self.olap_manager.query_cube(cube_name, dimensions, measures)
        
        output = io.BytesIO()
        if format == 'csv':
            df.to_csv(output, index=False, encoding='utf-8')
        elif format == 'excel':
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
        elif format == 'json':
            output.write(df.to_json(orient='records', indent=2, force_ascii=False).encode('utf-8'))
        elif format == 'parquet':
            df.to_parquet(output)
        elif format == 'html':
            output.write(df.to_html(index=False).encode('utf-8'))
        return output.getvalue()
    
    def export_to_power_bi(self, cube_name: str) -> bytes:
        """Экспорт для Power BI"""
        cube = self.olap_manager.cubes.get(cube_name)
        if not cube:
            return b''
        dimensions = list(cube.dimensions.keys())
        measures = [(m, 'SUM') for m in cube.measures.keys()]
        df = self.olap_manager.query_cube(cube_name, dimensions, measures)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            metadata = pd.DataFrame([
                {'Type': 'Dimension', 'Name': d, 'Column': cube.dimensions[d].column, 
                 'Hierarchy': str(cube.dimensions[d].hierarchy)} for d in dimensions
            ] + [
                {'Type': 'Measure', 'Name': m, 'Column': cube.measures[m].column, 
                 'Aggregation': cube.measures[m].default_agg, 'Unit': cube.measures[m].unit}
                for m in cube.measures.keys()
            ])
            metadata.to_excel(writer, sheet_name='Metadata', index=False)
        return output.getvalue()
    
    def get_cube_metadata(self, cube_name: str) -> Dict:
        """Метаданные куба"""
        cube = self.olap_manager.cubes.get(cube_name)
        if not cube:
            return {'error': 'Cube not found'}
        return {
            'name': cube.name, 'description': cube.description, 'table_name': cube.table_name,
            'dimensions': [{'name': n, 'column': d.column, 'hierarchy': d.hierarchy, 
                           'description': d.description, 'attributes': d.attributes}
                          for n, d in cube.dimensions.items()],
            'measures': [{'name': n, 'column': m.column, 'default_agg': m.default_agg, 
                         'description': m.description, 'format': m.format, 'unit': m.unit}
                        for n, m in cube.measures.items()],
            'calculated_members': cube.calculated_members, 'metadata': cube.metadata
        }
    
    def get_cubes_list(self) -> List[Dict]:
        """Список всех кубов"""
        return [{
            'name': name, 'description': cube.description,
            'dimension_count': len(cube.dimensions), 'measure_count': len(cube.measures),
            'table_name': cube.table_name
        } for name, cube in self.olap_manager.cubes.items()]
    
    def get_api_docs(self) -> Dict:
        """Документация API"""
        return {
            'version': APP_VERSION, 'title': 'OLAP Analytics Pro API',
            'description': 'REST API для многомерного анализа данных',
            'endpoints': {
                '/api/query': {'method': 'POST', 'description': 'Выполнение OLAP запроса',
                    'body': {'cube': 'string (required)', 'dimensions': 'array', 'measures': 'array',
                            'filters': 'object', 'aggregations': 'object', 'top_n': 'integer',
                            'order_by': 'array [[column, direction], ...]'}},
                '/api/export': {'method': 'POST', 'description': 'Экспорт данных',
                    'body': {'cube': 'string (required)', 'format': 'csv|excel|json|parquet|html',
                            'query': 'object'}},
                '/api/metadata/{cube}': {'method': 'GET', 'description': 'Метаданные куба'},
                '/api/cubes': {'method': 'GET', 'description': 'Список кубов'},
                '/api/mdx': {'method': 'POST', 'description': 'MDX-подобный запрос',
                    'body': {'cube': 'string (required)', 'mdx': 'string'}},
                '/api/powerbi/{cube}': {'method': 'GET', 'description': 'Экспорт для Power BI'}
            }
        }

# ============================================
# 13. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
class OLAPInterface:
    def __init__(self):
        self.conn = get_connection()
        self.olap_manager = OLAPManager(self.conn)
        self.user_manager = UserManager(self.conn)
        self.dashboard_manager = DashboardManager(self.olap_manager)
        self.api = OLAPAPI(self.olap_manager)
    
    def run(self):
        """Запуск приложения"""
        if not st.session_state.authenticated:
            self.render_login_page()
        else:
            self.render_main_interface()
    
    def render_login_page(self):
        """Страница входа"""
        st.markdown("<div class='login-container'>", unsafe_allow_html=True)
        st.markdown(f"## 🔐 OLAP Analytics Pro <small>v{APP_VERSION}</small>", unsafe_allow_html=True)
        st.markdown("---")
        
        username = st.text_input("👤 Логин", placeholder="admin", key="login_username")
        password = st.text_input("🔑 Пароль", type="password", placeholder="••••••••", key="login_password")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🚪 Войти", type="primary", use_container_width=True, key="btn_login"):
                if self.user_manager.authenticate(username, password):
                    st.success("✅ Успешный вход!")
                    time.sleep(0.5)
                    st.rerun()
        
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center; color: #666; font-size: 0.9em;'>
            <p><b>Демо-доступ:</b></p>
            <p>👑 admin / admin123 &nbsp;|&nbsp; 📊 analyst / analyst123 &nbsp;|&nbsp; 👁️ test / test123</p>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    def render_main_interface(self):
        """Основной интерфейс"""
        role_class = self.user_manager.get_user_role_class(st.session_state.role)
        
        st.markdown(f"""
        <div class='main-header'>
            <h1>🎲 OLAP Analytics Platform</h1>
            <p>
                <span class='connection-status status-online'></span>
                {st.session_state.username} 
                <span class='user-role-badge {role_class}'>{st.session_state.role}</span>
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.sidebar:
            st.markdown("## 🎯 Навигация")
            mode = st.radio("Режим работы", [
                "📊 Анализ", "📈 Дашборды", "🏗️ Конструктор", 
                "💾 Срезы", "⚙️ Администрирование", "🔌 API"
            ], key="nav_mode")
            
            if st.button("🚪 Выход", use_container_width=True, key="btn_logout"):
                username = st.session_state.get('username')
                # Очищаем чувствительные данные
                for key in ['authenticated', 'username', 'role', 'user_id', 'user_email', 'user_fullname', 'current_cube']:
                    st.session_state.pop(key, None)
                log_audit("LOGOUT", {"username": username})
                st.success("✅ Вы успешно вышли")
                st.rerun()
            
            st.markdown("---")
            self.render_sidebar_cubes()
            st.markdown("---")
            self.render_sidebar_stats()
            st.markdown("---")
            self.render_sidebar_filters()
        
        modes = {
            "📊 Анализ": self.render_analysis_mode,
            "📈 Дашборды": self.render_dashboard_mode,
            "🏗️ Конструктор": self.render_cube_designer,
            "💾 Срезы": self.render_slice_manager,
            "⚙️ Администрирование": self.render_admin_panel,
            "🔌 API": self.render_api_documentation
        }
        if mode in modes:
            modes[mode]()
    
    def render_sidebar_cubes(self):
        """Кубы в боковой панели"""
        st.markdown("### 📦 Кубы")
        cubes_df = self.olap_manager.get_cubes_list()
        
        if not cubes_df.empty:
            cube_names = cubes_df['name'].tolist()
            selected = st.selectbox("Выберите куб", cube_names, key="sidebar_cube_select")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🎲 Загрузить", use_container_width=True, key="btn_load_cube"):
                    if self.user_manager.check_permission(selected, 'READ'):
                        cube = self.olap_manager.load_cube(selected)
                        if cube:
                            st.session_state.current_cube = cube
                            st.success(f"✅ '{selected}' загружен")
                            st.rerun()
                    else:
                        st.error("❌ Нет доступа")
            with col2:
                if st.button("🔄", use_container_width=True, key="btn_refresh_cubes"):
                    st.rerun()
            
            if st.session_state.current_cube:
                st.info(f"📌 Активный: **{st.session_state.current_cube.name}**")
                info = self.olap_manager.get_table_info(st.session_state.current_cube.name)
                if info:
                    st.caption(f"📊 {info.get('row_count', 0):,} строк".replace(",", " "))
                    st.caption(f"📐 {info.get('dimension_count', 0)} изм., {info.get('measure_count', 0)} мер")
        else:
            st.info("Нет кубов. Создайте в Конструкторе.")
    
    def render_sidebar_stats(self):
        """Статистика в боковой панели"""
        with st.expander("📊 Статистика системы"):
            stats = self.olap_manager.query_cache.get_stats()
            cache_usage = (stats['size'] / stats['max_size']) * 100 if stats['max_size'] > 0 else 0
            st.markdown(f"""
            <div class='progress-container'>
                <div class='progress-bar' style='width: {cache_usage}%;'>{stats['size']}/{stats['max_size']}</div>
            </div>
            """, unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Hit Rate", stats['hit_rate'])
                st.metric("Попаданий", stats['hits'])
            with col2:
                st.metric("Память", f"{stats['memory_usage_mb']:.2f} MB")
                st.metric("Промахов", stats['misses'])
            if st.button("🗑️ Очистить кэш", use_container_width=True, key="btn_clear_cache"):
                self.olap_manager.query_cache.clear()
                st.success("Кэш очищен")
                st.rerun()
    
    def render_sidebar_filters(self):
        """Фильтры в боковой панели"""
        if st.session_state.current_cube:
            with st.expander("🔍 Фильтры"):
                cube = st.session_state.current_cube
                for dim_name, dim in cube.dimensions.items():
                    try:
                        values = self.conn.execute(
                            f'SELECT DISTINCT "{dim.column}" FROM "{cube.table_name}" ORDER BY "{dim.column}" LIMIT 100'
                        ).fetchdf()
                        if not values.empty:
                            selected = st.multiselect(
                                f"📌 {dim_name}", values[dim.column].tolist(),
                                key=f"filter_{dim_name}"
                            )
                            if selected:
                                st.session_state.filters[dim.column] = selected
                            elif dim.column in st.session_state.filters:
                                del st.session_state.filters[dim.column]
                    except:
                        pass
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🔄 Сбросить", use_container_width=True, key="btn_reset_filters"):
                        st.session_state.filters = {}
                        st.rerun()
                with col2:
                    if st.button("✅ Применить", use_container_width=True, type="primary", key="btn_apply_filters"):
                        st.rerun()
    
    def render_analysis_mode(self):
        """Режим анализа данных"""
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню")
            return
        cube = st.session_state.current_cube
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🎯 Сводная таблица", "📊 Визуализации", "🔍 Drill-down", "📋 Данные", "⚡ Оптимизация"
        ])
        with tab1: self.render_pivot_table(cube)
        with tab2: self.render_charts(cube)
        with tab3: self.render_drill_down(cube)
        with tab4: self.render_raw_data(cube)
        with tab5: self.render_optimization(cube)
    
    def render_pivot_table(self, cube: OLAPCube):
        """Продвинутая сводная таблица"""
        st.markdown("### 🎯 Интерактивная сводная таблица")
        if not cube.dimensions:
            st.warning("В кубе нет измерений"); return
        if not cube.measures:
            st.warning("В кубе нет мер"); return
        
        col1, col2 = st.columns(2)
        with col1:
            row_dims = st.multiselect("📌 Строки", list(cube.dimensions.keys()), key="pivot_rows")
        with col2:
            col_dims = st.multiselect("📌 Колонки", list(cube.dimensions.keys()), key="pivot_cols")
        measures = st.multiselect("📊 Меры", list(cube.measures.keys()), key="pivot_measures")
        
        with st.expander("⚙️ Расширенные настройки"):
            col_opt1, col_opt2, col_opt3 = st.columns(3)
            with col_opt1: top_n = st.number_input("Топ N", 0, 10000, 0, key="pivot_top_n")
            with col_opt2: export_format = st.selectbox("Экспорт", ["CSV", "Excel", "JSON", "Parquet"], key="pivot_export")
            with col_opt3: use_cache = st.checkbox("Кэшировать", True, key="pivot_cache")
        
        if st.button("🎯 Построить", type="primary", key="btn_build_pivot") and measures:
            with st.spinner("Выполнение запроса..."):
                pivot_df = self.olap_manager.slice_dice(
                    cube.name, row_dims, col_dims, measures, st.session_state.get('filters', {})
                )
                if not pivot_df.empty:
                    st.dataframe(pivot_df, use_container_width=True, height=600)
                    if export_format == "CSV":
                        csv = pivot_df.to_csv()
                        st.download_button("📥 Скачать CSV", csv, f"{cube.name}_pivot.csv", key="dl_csv")
                    elif export_format == "Excel":
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            pivot_df.to_excel(writer, sheet_name='Pivot')
                        st.download_button("📥 Скачать Excel", output.getvalue(), f"{cube.name}_pivot.xlsx", key="dl_xlsx")
                    elif export_format == "JSON":
                        json_str = pivot_df.to_json(orient='records', indent=2, force_ascii=False)
                        st.download_button("📥 Скачать JSON", json_str, f"{cube.name}_pivot.json", key="dl_json")
                    elif export_format == "Parquet":
                        output = io.BytesIO()
                        pivot_df.to_parquet(output)
                        st.download_button("📥 Скачать Parquet", output.getvalue(), f"{cube.name}_pivot.parquet", key="dl_parquet")
                else:
                    st.info("Нет данных для отображения")
    
    def render_charts(self, cube: OLAPCube):
        """Визуализации данных"""
        st.markdown("### 📊 Визуализации")
        chart_type = st.selectbox("Тип визуализации", [
            "Treemap", "Bar Chart", "Line Chart", "Pie Chart", "Heatmap", 
            "Scatter", "Waterfall", "Box Plot", "Histogram", "Area Chart"
        ], key="chart_type_select")
        figures = []
        
        if chart_type == "Treemap":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()), key="chart_treemap_dim")
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_treemap_meas")
            top_n = st.slider("Количество элементов", 5, 50, 20, key="chart_treemap_top")
            if st.button("Создать Treemap", key="btn_chart_treemap"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_treemap(cube.name, dim, measure, top_n)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Bar Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()), key="chart_bar_dim")
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_bar_meas")
            top_n = st.slider("Количество элементов", 5, 50, 10, key="chart_bar_top")
            horizontal = st.checkbox("Горизонтально", key="chart_bar_horiz")
            if st.button("Создать Bar Chart", key="btn_chart_bar"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, top_n, horizontal)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Line Chart":
            date_dims = [d for d, dim in cube.dimensions.items() if dim.hierarchy]
            if date_dims:
                dim = st.selectbox("Измерение даты", date_dims, key="chart_line_dim")
                measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_line_meas")
                if st.button("Создать Line Chart", key="btn_chart_line"):
                    with st.spinner("Создание..."):
                        fig = self.dashboard_manager.create_line_chart(cube.name, dim, measure)
                        if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            else: st.info("Нет измерений с иерархией дат")
        elif chart_type == "Pie Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()), key="chart_pie_dim")
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_pie_meas")
            top_n = st.slider("Количество элементов", 3, 20, 8, key="chart_pie_top")
            if st.button("Создать Pie Chart", key="btn_chart_pie"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_pie_chart(cube.name, dim, measure, top_n)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Heatmap":
            if len(cube.dimensions) >= 2:
                row_dim = st.selectbox("Строки", list(cube.dimensions.keys()), key="chart_heat_row")
                col_dim = st.selectbox("Колонки", list(cube.dimensions.keys()), key="chart_heat_col")
                measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_heat_meas")
                if st.button("Создать Heatmap", key="btn_chart_heat"):
                    with st.spinner("Создание..."):
                        fig = self.dashboard_manager.create_heatmap(cube.name, row_dim, col_dim, measure)
                        if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            else: st.info("Нужно минимум 2 измерения")
        elif chart_type == "Scatter":
            if len(cube.measures) >= 2:
                x_measure = st.selectbox("X", list(cube.measures.keys()), key="chart_scatter_x")
                y_measure = st.selectbox("Y", list(cube.measures.keys()), key="chart_scatter_y")
                color_dim = st.selectbox("Цвет", ["Нет"] + list(cube.dimensions.keys()), key="chart_scatter_color")
                size_measure = st.selectbox("Размер", ["Нет"] + list(cube.measures.keys()), key="chart_scatter_size")
                if st.button("Создать Scatter Plot", key="btn_chart_scatter"):
                    with st.spinner("Создание..."):
                        fig = self.dashboard_manager.create_scatter_plot(
                            cube.name, x_measure, y_measure,
                            color_dim if color_dim != "Нет" else None,
                            size_measure if size_measure != "Нет" else None
                        )
                        if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            else: st.info("Нужно минимум 2 меры")
        elif chart_type == "Waterfall":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()), key="chart_wf_dim")
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_wf_meas")
            if st.button("Создать Waterfall", key="btn_chart_wf"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_waterfall(cube.name, dim, measure)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Box Plot":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()), key="chart_box_dim")
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_box_meas")
            if st.button("Создать Box Plot", key="btn_chart_box"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_box_plot(cube.name, dim, measure)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Histogram":
            measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_hist_meas")
            bins = st.slider("Количество столбцов", 5, 100, 20, key="chart_hist_bins")
            if st.button("Создать Histogram", key="btn_chart_hist"):
                with st.spinner("Создание..."):
                    fig = self.dashboard_manager.create_histogram(cube.name, measure, bins)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
        elif chart_type == "Area Chart":
            date_dims = [d for d, dim in cube.dimensions.items() if dim.hierarchy]
            if date_dims:
                dim = st.selectbox("Измерение даты", date_dims, key="chart_area_dim")
                measure = st.selectbox("Мера", list(cube.measures.keys()), key="chart_area_meas")
                if st.button("Создать Area Chart", key="btn_chart_area"):
                    with st.spinner("Создание..."):
                        fig = self.dashboard_manager.create_area_chart(cube.name, dim, measure)
                        if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            else: st.info("Нет измерений с иерархией дат")
        
        if figures:
            st.session_state.chart_figures = figures
    
    def render_drill_down(self, cube: OLAPCube):
        """Drill-down анализ"""
        st.markdown("### 🔍 Drill-down по иерархиям")
        hierarchical_dims = {name: dim for name, dim in cube.dimensions.items() if dim.hierarchy}
        if hierarchical_dims:
            dim_name = st.selectbox("Измерение", list(hierarchical_dims.keys()), key="drill_dim_select")
            dim = hierarchical_dims[dim_name]
            st.markdown(f"**Иерархия:** {' → '.join(dim.hierarchy)}")
            current_level = len(st.session_state.drill_path)
            if current_level < len(dim.hierarchy):
                next_level = dim.hierarchy[current_level]
                if st.button(f"⬇️ Drill down: {next_level}", key=f"btn_drill_{next_level}"):
                    query = f"""
                        SELECT DATE_PART('{next_level.lower()}', "{dim.column}") as {next_level},
                               COUNT(*) as count FROM "{cube.table_name}"
                    """
                    if st.session_state.drill_path:
                        for i, val in enumerate(st.session_state.drill_path):
                            level = dim.hierarchy[i].lower()
                            query += f" WHERE DATE_PART('{level}', \"{dim.column}\") = '{val}'"
                    query += f" GROUP BY {next_level} ORDER BY {next_level}"
                    try:
                        df = self.conn.execute(query).fetchdf()
                        st.dataframe(df, use_container_width=True)
                        if not df.empty:
                            selected_value = st.selectbox(f"Выберите {next_level}", df[next_level].tolist(), key=f"drill_val_{next_level}")
                            if st.button("Продолжить drill-down", key="btn_drill_continue"):
                                st.session_state.drill_path.append(selected_value)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка drill-down: {e}")
            if st.session_state.drill_path:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("⬆️ Drill up", key="btn_drill_up"):
                        st.session_state.drill_path.pop()
                        st.rerun()
                with col2:
                    if st.button("🔄 Сбросить", key="btn_drill_reset"):
                        st.session_state.drill_path = []
                        st.rerun()
                st.markdown(f"**Текущий путь:** {' → '.join(st.session_state.drill_path)}")
        else:
            st.info("Нет измерений с иерархиями")
    
    def render_raw_data(self, cube: OLAPCube):
        """Просмотр сырых данных"""
        st.markdown("### 📋 Данные куба")
        try:
            count = self.conn.execute(f"SELECT COUNT(*) FROM \"{cube.table_name}\"").fetchone()[0]
            st.metric("Всего строк", f"{count:,}".replace(",", " "))
            limit = st.slider("Количество строк", 10, 10000, 1000, key="raw_data_limit")
            df = self.conn.execute(f"SELECT * FROM \"{cube.table_name}\" LIMIT {limit}").fetchdf()
            st.dataframe(df, use_container_width=True, height=500)
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("📥 CSV", key="btn_export_raw_csv"):
                    all_df = self.conn.execute(f"SELECT * FROM \"{cube.table_name}\"").fetchdf()
                    csv = all_df.to_csv(index=False)
                    st.download_button("Скачать CSV", csv, f"{cube.name}_data.csv", key="dl_raw_csv")
            with col2:
                if st.button("📥 Excel", key="btn_export_raw_xlsx"):
                    all_df = self.conn.execute(f"SELECT * FROM \"{cube.table_name}\"").fetchdf()
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        all_df.to_excel(writer, sheet_name='Data', index=False)
                    st.download_button("Скачать Excel", output.getvalue(), f"{cube.name}_data.xlsx", key="dl_raw_xlsx")
            with col3:
                if st.button("📥 Parquet", key="btn_export_raw_parquet"):
                    all_df = self.conn.execute(f"SELECT * FROM \"{cube.table_name}\"").fetchdf()
                    output = io.BytesIO()
                    all_df.to_parquet(output)
                    st.download_button("Скачать Parquet", output.getvalue(), f"{cube.name}_data.parquet", key="dl_raw_parquet")
        except Exception as e:
            st.error(f"Ошибка: {e}")
    
    def render_optimization(self, cube: OLAPCube):
        """Панель оптимизации"""
        st.markdown("### ⚡ Оптимизация производительности")
        st.markdown("#### 📊 Статистика запросов")
        stats_df = self.olap_manager.get_query_performance_stats()
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
        else:
            st.info("Нет данных о запросах")
        st.markdown("#### 💾 Материализованные представления")
        col1, col2 = st.columns(2)
        with col1:
            mv_dims = st.multiselect("Измерения", list(cube.dimensions.keys()), key="mv_dims")
        with col2:
            mv_measures = st.multiselect("Меры", list(cube.measures.keys()), key="mv_measures")
        if mv_measures:
            view_name = st.text_input("Название", f"MV_{datetime.now().strftime('%Y%m%d_%H%M%S')}", key="mv_name")
            if st.button("Создать материализованное представление", key="btn_create_mv"):
                with st.spinner("Создание..."):
                    table_name = self.olap_manager.create_materialized_view(cube.name, view_name, mv_dims, mv_measures)
                    st.success(f"✅ Создано: {table_name}")
        st.markdown("#### 🗑️ Очистка")
        if st.button("Очистить кэш запросов", key="btn_clear_query_cache"):
            self.olap_manager.query_cache.clear()
            st.success("Кэш очищен")
    
    def render_dashboard_mode(self):
        """Режим дашбордов"""
        st.markdown("### 📈 Интерактивные дашборды")
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню"); return
        cube = st.session_state.current_cube
        tab1, tab2, tab3 = st.tabs(["📊 Текущий дашборд", "💾 Сохранить", "📂 Загрузить"])
        with tab1:
            st.markdown("#### 🎯 Ключевые показатели")
            measures = list(cube.measures.keys())[:4]
            if measures:
                cols = st.columns(len(measures))
                kpis = self.dashboard_manager.create_kpi_cards(cube.name, measures)
                for i, (measure, values) in enumerate(kpis.items()):
                    with cols[i]:
                        formatted_value = f"{values['current']:,.0f}"
                        if values.get('unit'): formatted_value += f" {values['unit']}"
                        st.markdown(f"""
                        <div class='kpi-card'>
                            <div class='kpi-value'>{formatted_value}</div>
                            <div class='kpi-label'>{measure}</div>
                        </div>
                        """, unsafe_allow_html=True)
            st.markdown("#### 📊 Визуализации")
            chart_cols = st.columns(2); figures = []
            with chart_cols[0]:
                if len(cube.dimensions) > 0 and len(cube.measures) > 0:
                    dim = list(cube.dimensions.keys())[0]; measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, 10)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            with chart_cols[1]:
                if len(cube.dimensions) > 1 and len(cube.measures) > 0:
                    dim1, dim2 = list(cube.dimensions.keys())[0], list(cube.dimensions.keys())[1]
                    measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_heatmap(cube.name, dim1, dim2, measure)
                    if fig: st.plotly_chart(fig, use_container_width=True); figures.append(fig)
            if figures or st.session_state.get('chart_figures'):
                all_figures = figures + st.session_state.get('chart_figures', [])
                if st.button("📥 Экспортировать дашборд в HTML", key="btn_export_dashboard_html"):
                    html_content = self.dashboard_manager.export_dashboard_to_html(all_figures, f"{cube.name} Dashboard")
                    st.download_button("Скачать HTML", html_content, f"{cube.name}_dashboard.html", "text/html", key="dl_dash_html")
        with tab2:
            st.markdown("#### 💾 Сохранить дашборд")
            dash_name = st.text_input("Название дашборда", key="dash_save_name")
            dash_desc = st.text_area("Описание", key="dash_save_desc")
            dash_config = {
                'cube': cube.name, 'measures': st.session_state.get('pivot_measures', []),
                'dimensions': st.session_state.get('pivot_rows', []) + st.session_state.get('pivot_cols', []),
                'filters': st.session_state.get('filters', {})
            }
            if st.button("💾 Сохранить дашборд", type="primary", key="btn_save_dashboard") and dash_name:
                if self.dashboard_manager.save_dashboard(dash_name, cube.name, dash_config):
                    st.success("✅ Дашборд сохранен")
                else:
                    st.error("❌ Ошибка сохранения")
        with tab3:
            st.markdown("#### 📂 Сохраненные дашборды")
            dashboards = self.dashboard_manager.load_dashboards(cube.name)
            if not dashboards.empty:
                for _, row in dashboards.iterrows():
                    with st.expander(f"{row['name']} - {row['created_at']}"):
                        st.markdown(f"**Владелец:** {row['owner']}")
                        if row['config']:
                            try: st.json(json.loads(row['config']))
                            except: pass
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("📂 Загрузить", key=f"load_dash_{row['id']}"):
                                st.success("✅ Дашборд загружен")
                        with col2:
                            if st.button("🗑️ Удалить", key=f"del_dash_{row['id']}"):
                                if self.dashboard_manager.delete_dashboard(row['id']):
                                    st.success("✅ Дашборд удален"); st.rerun()
            else:
                st.info("Нет сохраненных дашбордов")
    
    def render_cube_designer(self):
        """Конструктор кубов"""
        st.markdown("### 🏗️ Конструктор OLAP кубов")
        if not self.user_manager.check_permission('*', 'WRITE'):
            st.error("❌ Недостаточно прав для создания кубов"); return
        tab1, tab2, tab3 = st.tabs(["📤 Создать куб", "📋 Управление", "🔧 Редактор"])
        with tab1:
            uploaded_files = st.file_uploader("Загрузите данные", type=['csv', 'xlsx', 'xls', 'parquet'], accept_multiple_files=True, key="cube_upload")
            if uploaded_files:
                cube_name = st.text_input("Название куба", f"Cube_{datetime.now().strftime('%Y%m%d_%H%M')}", key="cube_name_input")
                cube_desc = st.text_area("Описание", "", key="cube_desc_input")
                dfs = []
                for file in uploaded_files:
                    try:
                        if file.name.endswith('.csv'): df = pd.read_csv(file)
                        elif file.name.endswith('.parquet'): df = pd.read_parquet(file)
                        else: df = pd.read_excel(file)
                        dfs.append(df)
                        st.success(f"✅ {file.name}: {len(df):,} строк")
                    except Exception as e:
                        st.error(f"Ошибка загрузки {file.name}: {e}")
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    st.markdown("**Предпросмотр:**")
                    st.dataframe(combined_df.head(10), use_container_width=True)
                    st.markdown(f"**Размер данных:** {len(combined_df):,} строк, {len(combined_df.columns)} колонок")
                    with st.expander("🔧 Ручная настройка"):
                        st.markdown("**Измерения:**")
                        dimensions = st.multiselect("Выберите измерения", combined_df.columns,
                            default=[c for c in combined_df.columns[:5] if combined_df[c].dtype == 'object'], key="cube_dims_select")
                        st.markdown("**Меры:**")
                        measures = st.multiselect("Выберите меры", combined_df.columns,
                            default=[c for c in combined_df.columns if pd.api.types.is_numeric_dtype(combined_df[c])], key="cube_meas_select")
                    if st.button("🎲 Создать куб", type="primary", key="btn_create_cube"):
                        with st.spinner("Создание куба и оптимизация..."):
                            cube = self.olap_manager.create_cube_from_dataframe(cube_name, combined_df, cube_desc, auto_detect=True)
                            if cube:
                                st.session_state.current_cube = cube
                                st.success(f"✅ Куб '{cube_name}' создан!")
                                col1, col2, col3, col4 = st.columns(4)
                                with col1: st.metric("Строк", f"{len(combined_df):,}")
                                with col2: st.metric("Колонок", len(combined_df.columns))
                                with col3: st.metric("Измерений", len(cube.dimensions))
                                with col4: st.metric("Мер", len(cube.measures))
        with tab2:
            cubes_df = self.olap_manager.get_cubes_list()
            if not cubes_df.empty:
                st.dataframe(cubes_df, use_container_width=True)
                st.markdown("---"); st.markdown("### 🗑️ Удаление куба")
                col1, col2 = st.columns([3, 1])
                with col1: to_delete = st.selectbox("Выберите куб", cubes_df['name'].tolist(), key="cube_delete_select")
                with col2:
                    if st.button("🗑️ Удалить", type="secondary", use_container_width=True, key="btn_delete_cube"):
                        if self.user_manager.check_permission(to_delete, 'WRITE'):
                            if self.olap_manager.delete_cube(to_delete):
                                st.success(f"✅ Куб '{to_delete}' удалён")
                                if st.session_state.current_cube and st.session_state.current_cube.name == to_delete:
                                    st.session_state.current_cube = None
                                st.rerun()
                            else: st.error("❌ Ошибка удаления")
                        else: st.error("❌ Недостаточно прав")
            else: st.info("Нет созданных кубов")
        with tab3:
            if st.session_state.current_cube:
                cube = st.session_state.current_cube
                st.markdown(f"### 🔧 Редактор куба: {cube.name}")
                st.markdown("#### Измерения")
                for dim_name, dim in cube.dimensions.items():
                    st.markdown(f"- **{dim_name}**: `{dim.column}` (иерархия: {dim.hierarchy})")
                st.markdown("#### Меры")
                for measure_name, measure in cube.measures.items():
                    st.markdown(f"- **{measure_name}**: `{measure.column}` (агрегация: {measure.default_agg})")
            else: st.info("Загрузите куб для редактирования")
    
    def render_slice_manager(self):
        """Управление срезами данных"""
        st.markdown("### 💾 Управление срезами данных")
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню"); return
        cube = st.session_state.current_cube
        tab1, tab2 = st.tabs(["💾 Сохранить срез", "📂 Загрузить срез"])
        with tab1:
            st.markdown("#### 💾 Сохранить текущий срез")
            slice_name = st.text_input("Название среза", key="slice_save_name")
            slice_desc = st.text_area("Описание", key="slice_save_desc")
            current_config = {
                'cube': cube.name, 'filters': st.session_state.get('filters', {}),
                'rows': st.session_state.get('pivot_rows', []), 'cols': st.session_state.get('pivot_cols', []),
                'measures': st.session_state.get('pivot_measures', []), 'drill_path': st.session_state.get('drill_path', [])
            }
            st.json(current_config)
            if st.button("💾 Сохранить срез", type="primary", key="btn_save_slice") and slice_name:
                try:
                    max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_slices").fetchone()[0]
                    self.conn.execute("""
                        INSERT INTO olap_slices (id, cube_name, slice_name, definition, description, owner)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [max_id + 1, cube.name, slice_name, json.dumps(current_config, default=str), slice_desc, st.session_state.username])
                    st.success("✅ Срез сохранен!")
                    log_audit("SAVE_SLICE", {"cube": cube.name, "slice": slice_name})
                except Exception as e:
                    st.error(f"Ошибка сохранения: {e}")
        with tab2:
            st.markdown("#### 📂 Сохраненные срезы")
            try:
                slices_df = self.conn.execute("""
                    SELECT id, slice_name, definition, description, created_at, owner
                    FROM olap_slices WHERE cube_name = ? ORDER BY created_at DESC
                """, [cube.name]).fetchdf()
                if not slices_df.empty:
                    for _, row in slices_df.iterrows():
                        with st.expander(f"{row['slice_name']} - {row['created_at']}"):
                            slice_def = json.loads(row['definition'])
                            st.markdown(f"**Владелец:** {row['owner']}")
                            st.markdown(f"**Описание:** {row['description'] or 'Нет'}")
                            st.json(slice_def)
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("📂 Загрузить", key=f"load_slice_{row['id']}"):
                                    st.session_state.filters = slice_def.get('filters', {})
                                    st.session_state.pivot_rows = slice_def.get('rows', [])
                                    st.session_state.pivot_cols = slice_def.get('cols', [])
                                    st.session_state.pivot_measures = slice_def.get('measures', [])
                                    st.session_state.drill_path = slice_def.get('drill_path', [])
                                    st.success("✅ Срез загружен")
                                    log_audit("LOAD_SLICE", {"cube": cube.name, "slice": row['slice_name']})
                                    st.rerun()
                            with col2:
                                if st.button("🗑️ Удалить", key=f"del_slice_{row['id']}"):
                                    self.conn.execute("DELETE FROM olap_slices WHERE id = ?", [row['id']])
                                    st.success("✅ Срез удален")
                                    log_audit("DELETE_SLICE", {"slice_id": row['id']})
                                    st.rerun()
                else: st.info("Нет сохраненных срезов")
            except Exception as e:
                st.error(f"Ошибка загрузки срезов: {e}")
    
    def render_admin_panel(self):
        """Административная панель"""
        st.markdown("### ⚙️ Администрирование системы")
        if st.session_state.get('role') != 'ADMIN':
            st.error("❌ Доступ только для администраторов"); return
        admin_tabs = st.tabs(["👥 Пользователи", "🔐 Права доступа", "📊 Мониторинг", "🗄️ База данных", "📝 Аудит", "⚙️ Настройки"])
        with admin_tabs[0]:
            st.markdown("#### 👥 Управление пользователями")
            with st.expander("➕ Создать пользователя"):
                col1, col2 = st.columns(2)
                with col1:
                    new_username = st.text_input("Логин", key="admin_new_username")
                    new_password = st.text_input("Пароль", type="password", key="admin_new_password")
                with col2:
                    new_role = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"], key="admin_new_role")
                    new_email = st.text_input("Email", key="admin_new_email")
                new_fullname = st.text_input("Полное имя", key="admin_new_fullname")
                if st.button("Создать пользователя", key="btn_admin_create_user"):
                    if self.user_manager.create_user(new_username, new_password, new_role, new_email, new_fullname):
                        st.success("✅ Пользователь создан"); st.rerun()
                    else: st.error("❌ Ошибка создания")
            users_df = self.user_manager.get_users_list()
            if not users_df.empty:
                st.dataframe(users_df, use_container_width=True)
                st.markdown("---"); st.markdown("#### ✏️ Редактировать пользователя")
                selected_user = st.selectbox("Выберите пользователя", users_df['username'].tolist(), key="admin_edit_user_select")
                user_data = users_df[users_df['username'] == selected_user].iloc[0]
                col1, col2, col3 = st.columns(3)
                with col1:
                    new_role_edit = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"], 
                                                index=["VIEWER", "ANALYST", "ADMIN"].index(user_data['role']), key="admin_edit_role")
                with col2:
                    new_email_edit = st.text_input("Email", value=user_data.get('email', ''), key="admin_edit_email")
                with col3:
                    is_active = st.checkbox("Активен", value=user_data.get('is_active', True), key="admin_edit_active")
                new_fullname_edit = st.text_input("Полное имя", value=user_data.get('full_name', ''), key="admin_edit_fullname")
                new_password_edit = st.text_input("Новый пароль (оставьте пустым)", type="password", key="admin_edit_password")
                if st.button("Обновить пользователя", key="btn_admin_update_user"):
                    if self.user_manager.update_user(selected_user, role=new_role_edit, email=new_email_edit,
                            full_name=new_fullname_edit, password=new_password_edit if new_password_edit else None, is_active=is_active):
                        st.success("✅ Пользователь обновлен"); st.rerun()
                    else: st.error("❌ Ошибка обновления")
                if selected_user != 'admin':
                    if st.button("🗑️ Удалить пользователя", type="secondary", key="btn_admin_delete_user"):
                        if self.user_manager.delete_user(selected_user):
                            st.success("✅ Пользователь удален"); st.rerun()
                        else: st.error("❌ Ошибка удаления")
        with admin_tabs[1]:
            st.markdown("#### 🔐 Управление правами доступа")
            cubes = self.olap_manager.get_cubes_list()
            st.markdown("##### Назначить права")
            col1, col2, col3 = st.columns(3)
            with col1: role = st.selectbox("Роль", ["VIEWER", "ANALYST"], key="admin_perm_role")
            with col2: cube = st.selectbox("Куб", ['*'] + (cubes['name'].tolist() if not cubes.empty else []), key="admin_perm_cube")
            with col3: access = st.selectbox("Уровень доступа", ["READ", "WRITE"], key="admin_perm_level")
            if st.button("Назначить права", key="btn_admin_grant_perm"):
                if self.user_manager.grant_permission(role, cube, access):
                    st.success("✅ Права назначены"); st.rerun()
                else: st.error("❌ Ошибка назначения прав")
            st.markdown("---"); st.markdown("##### Текущие права")
            perms_df = self.user_manager.get_permissions_list()
            if not perms_df.empty:
                st.dataframe(perms_df, use_container_width=True)
                st.markdown("##### Отозвать права")
                col1, col2 = st.columns(2)
                with col1:
                    revoke_role = st.selectbox("Роль", perms_df['user_role'].unique(), key="admin_revoke_role")
                with col2:
                    revoke_cube = st.selectbox("Куб", 
                                               perms_df[perms_df['user_role'] == revoke_role]['cube_name'].tolist(),
                                               key="admin_revoke_cube")
                if st.button("Отозвать права", key="btn_admin_revoke_perm"):
                    if self.user_manager.revoke_permission(revoke_role, revoke_cube):
                        st.success("✅ Права отозваны"); st.rerun()
                    else: st.error("❌ Ошибка отзыва прав")
        with admin_tabs[2]:
            st.markdown("#### 📊 Мониторинг системы")
            st.markdown("**Статистика запросов:**")
            stats_df = self.olap_manager.get_query_performance_stats()
            if not stats_df.empty: st.dataframe(stats_df, use_container_width=True)
            st.markdown("**Состояние кэша:**")
            cache_stats = self.olap_manager.query_cache.get_stats()
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Записей", cache_stats['size'])
            with col2: st.metric("Hit Rate", cache_stats['hit_rate'])
            with col3: st.metric("Попаданий", cache_stats['hits'])
            with col4: st.metric("Промахов", cache_stats['misses'])
            st.markdown("**Последние запросы:**")
            try:
                recent = self.conn.execute("""
                    SELECT timestamp, user_name, cube_name, ROUND(execution_time, 3) as exec_time, rows_returned, status
                    FROM query_history ORDER BY timestamp DESC LIMIT 20
                """).fetchdf()
                if not recent.empty: st.dataframe(recent, use_container_width=True)
            except: pass
        with admin_tabs[3]:
            st.markdown("#### 🗄️ Управление базой данных")
            if st.button("📊 Показать все таблицы", key="btn_admin_show_tables"):
                try:
                    tables = self.conn.execute("SHOW TABLES").fetchdf()
                    st.dataframe(tables, use_container_width=True)
                except: st.info("Не удалось получить список таблиц")
            if st.button("🗜️ Оптимизировать базу (VACUUM)", key="btn_admin_vacuum"):
                try:
                    self.conn.execute("VACUUM"); st.success("✅ База данных оптимизирована")
                except: st.error("❌ Ошибка оптимизации")
            if os.path.exists(DB_PATH):
                size = os.path.getsize(DB_PATH) / 1024 / 1024
                st.metric("Размер файла БД", f"{size:.2f} MB")
            st.markdown("---"); st.markdown("#### ⚠️ Опасная зона")
            if st.button("🗑️ Очистить историю запросов", type="secondary", key="btn_admin_clear_history"):
                try:
                    self.conn.execute("DELETE FROM query_history"); st.success("✅ История запросов очищена")
                except: st.error("❌ Ошибка очистки")
        with admin_tabs[4]:
            st.markdown("#### 📝 Журнал аудита")
            try:
                audit_df = self.conn.execute("""
                    SELECT timestamp, user_name, action, details FROM audit_log ORDER BY timestamp DESC LIMIT 100
                """).fetchdf()
                if not audit_df.empty: st.dataframe(audit_df, use_container_width=True)
                else: st.info("Журнал аудита пуст")
            except: st.info("Таблица аудита не найдена")
        with admin_tabs[5]:
            st.markdown("#### ⚙️ Настройки системы")
            try:
                settings = self.conn.execute("SELECT key, value FROM system_settings").fetchdf()
                if not settings.empty:
                    for _, row in settings.iterrows():
                        st.text_input(row['key'], value=row['value'], key=f"setting_{row['key']}")
                if st.button("Сохранить настройки", key="btn_admin_save_settings"):
                    st.success("✅ Настройки сохранены")
            except: st.info("Таблица настроек не найдена")
    
    def render_api_documentation(self):
        """Документация API"""
        st.markdown("### 🔌 API для внешних систем")
        docs = self.api.get_api_docs()
        st.markdown(f"### {docs['title']}")
        st.markdown(f"**Версия:** {docs['version']}")
        st.markdown(f"*{docs['description']}*")
        st.markdown("---")
        for endpoint, info in docs['endpoints'].items():
            with st.expander(f"{info['method']} {endpoint}"):
                st.markdown(f"**Описание:** {info['description']}")
                st.markdown("**Параметры запроса:**")
                st.json(info.get('body', {}))
        st.markdown("---"); st.markdown("#### 🧪 Тестирование API")
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            st.markdown("**Метаданные текущего куба:**")
            meta = self.api.get_cube_metadata(cube.name)
            st.json(meta)
            st.markdown("---"); st.markdown("**MDX запрос:**")
            if cube.measures and cube.dimensions:
                mdx_query = st.text_area("MDX Запрос",
                    value=f"""SELECT 
  {{[Measures].[{list(cube.measures.keys())[0]}]}} ON COLUMNS,
  {{[Dimension].[{list(cube.dimensions.keys())[0]}]}} ON ROWS
FROM [{cube.name}]""", height=150, key="api_mdx_query")
                if st.button("Выполнить MDX запрос", key="btn_api_exec_mdx"):
                    result = self.api.execute_mdx_query(cube.name, mdx_query)
                    if result.get('error'): st.error(f"Ошибка: {result['error']}")
                    else:
                        st.success(f"Получено {len(result.get('result', []))} записей")
                        st.json(result)
            st.markdown("---"); st.markdown("**Экспорт данных:**")
            col1, col2 = st.columns(2)
            with col1: export_format = st.selectbox("Формат", ["csv", "excel", "json", "parquet", "html"], key="api_export_format")
            with col2:
                if st.button("📥 Экспортировать", key="btn_api_export"):
                    data = self.api.export_data(cube.name, export_format)
                    if data:
                        st.download_button("Скачать файл", data, f"{cube.name}_export.{export_format}", "application/octet-stream", key="dl_api_export")
            st.markdown("---"); st.markdown("**Экспорт для Power BI:**")
            if st.button("📊 Экспортировать для Power BI", key="btn_api_powerbi"):
                data = self.api.export_to_power_bi(cube.name)
                if data:
                    st.download_button("Скачать Power BI файл", data, f"{cube.name}_powerbi.xlsx", 
                                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_api_powerbi")
            st.markdown("---"); st.markdown("**Список всех кубов:**")
            cubes_list = self.api.get_cubes_list()
            st.json(cubes_list)
        else:
            st.info("👈 Загрузите куб для тестирования API")

# ============================================
# 14. ЗАПУСК ПРИЛОЖЕНИЯ
# ============================================
def main():
    interface = OLAPInterface()
    interface.run()

if __name__ == "__main__":
    main()
```
