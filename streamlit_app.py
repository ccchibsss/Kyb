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
warnings.filterwarnings('ignore')

# ============================================
# 1. НАСТРОЙКА СТРАНИЦЫ
# ============================================
st.set_page_config(
    page_title="OLAP Analytics Pro",
    page_icon="🎲",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# 2. ИНИЦИАЛИЗАЦИЯ БД И АДМИНА
# ============================================
if 'db_initialized' not in st.session_state:
    if os.path.exists('olap_analytics.db'):
        try:
            os.remove('olap_analytics.db')
        except:
            pass
    st.session_state.db_initialized = True

@st.cache_resource
def get_connection():
    conn = duckdb.connect('olap_analytics.db')
    conn.execute("INSTALL json; LOAD json;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    return conn

conn = get_connection()

def init_database():
    """Полная инициализация базы данных"""
    try:
        # Таблица пользователей
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username VARCHAR NOT NULL,
                password_hash VARCHAR NOT NULL,
                role VARCHAR DEFAULT 'VIEWER',
                email VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        except:
            pass
        
        # Таблица прав доступа
        conn.execute("""
            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY,
                user_role VARCHAR NOT NULL,
                cube_name VARCHAR NOT NULL,
                access_level VARCHAR DEFAULT 'READ',
                granted_by VARCHAR,
                granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_permissions_role_cube ON permissions(user_role, cube_name)")
        except:
            pass
        
        # Таблица кубов
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olap_cubes (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                definition JSON,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                owner VARCHAR,
                is_public BOOLEAN DEFAULT FALSE
            )
        """)
        
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cubes_name ON olap_cubes(name)")
        except:
            pass
        
        # Таблица срезов
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olap_slices (
                id INTEGER PRIMARY KEY,
                cube_name VARCHAR NOT NULL,
                slice_name VARCHAR NOT NULL,
                definition JSON,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                owner VARCHAR
            )
        """)
        
        # Таблица истории запросов
        conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY,
                cube_name VARCHAR,
                query_text VARCHAR,
                execution_time FLOAT,
                rows_returned INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                user_name VARCHAR,
                status VARCHAR DEFAULT 'SUCCESS'
            )
        """)
        
        # Таблица партиций
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
        
        # Таблица дашбордов
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dashboards (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                cube_name VARCHAR NOT NULL,
                config JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                owner VARCHAR
            )
        """)
        
        # Таблица scheduled reports
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                owner VARCHAR,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # Создаём админа
        admin_exists = conn.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'").fetchone()[0]
        
        if admin_exists == 0:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
            admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
            conn.execute("""
                INSERT INTO users (id, username, password_hash, role, email, is_active)
                VALUES (?, 'admin', ?, 'ADMIN', 'admin@olap.local', TRUE)
            """, [max_id + 1, admin_hash])
        else:
            admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
            conn.execute("UPDATE users SET password_hash = ?, role = 'ADMIN' WHERE username = 'admin'", [admin_hash])
        
        # Права админа
        perm_exists = conn.execute(
            "SELECT COUNT(*) FROM permissions WHERE user_role = 'ADMIN' AND cube_name = '*'"
        ).fetchone()[0]
        
        if perm_exists == 0:
            max_perm_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
            conn.execute("""
                INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by)
                VALUES (?, 'ADMIN', '*', 'ADMIN', 'system')
            """, [max_perm_id + 1])
        
        return True
    except Exception as e:
        st.error(f"Ошибка инициализации БД: {e}")
        return False

init_database()

# ============================================
# 3. CSS СТИЛИ
# ============================================
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        margin-bottom: 20px;
    }
    
    .olap-card {
        background: white;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 15px;
    }
    
    .dimension-badge {
        background: #e3f2fd;
        color: #1976d2;
        padding: 5px 10px;
        border-radius: 20px;
        font-size: 0.9em;
        margin: 2px;
        display: inline-block;
    }
    
    .measure-badge {
        background: #fce4ec;
        color: #c2185b;
        padding: 5px 10px;
        border-radius: 20px;
        font-size: 0.9em;
        margin: 2px;
        display: inline-block;
    }
    
    .dashboard-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        margin: 10px 0;
    }
    
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
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
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
    
    .dataframe th {
        background: #1e3c72 !important;
        color: white !important;
        padding: 10px !important;
    }
    
    .user-role-badge {
        background: #4caf50;
        color: white;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.8em;
    }
    
    .login-container {
        max-width: 400px;
        margin: 100px auto;
        padding: 30px;
        background: white;
        border-radius: 10px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
    }
    
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    
    .kpi-value {
        font-size: 2.5em;
        font-weight: bold;
    }
    
    .kpi-label {
        font-size: 0.9em;
        opacity: 0.9;
    }
    
    .alert-success {
        background: #d4edda;
        color: #155724;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #28a745;
    }
    
    .alert-error {
        background: #f8d7da;
        color: #721c24;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #dc3545;
    }
    
    .alert-warning {
        background: #fff3cd;
        color: #856404;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #ffc107;
    }
    
    .alert-info {
        background: #d1ecf1;
        color: #0c5460;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #17a2b8;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 4. КЭШИРОВАНИЕ ЗАПРОСОВ
# ============================================
class QueryCache:
    def __init__(self):
        self.cache = {}
        self.cache_stats = {'hits': 0, 'misses': 0}
        
    def get_cache_key(self, query: str, params: tuple = ()) -> str:
        content = query + str(params)
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        if key in self.cache:
            self.cache_stats['hits'] += 1
            return self.cache[key]['data'].copy()
        self.cache_stats['misses'] += 1
        return None
    
    def set(self, key: str, data: pd.DataFrame, ttl: int = 3600):
        self.cache[key] = {
            'data': data.copy(),
            'timestamp': datetime.now(),
            'ttl': ttl
        }
        self._cleanup()
    
    def _cleanup(self):
        now = datetime.now()
        expired_keys = [
            key for key, value in self.cache.items()
            if (now - value['timestamp']).seconds > value['ttl']
        ]
        for key in expired_keys:
            del self.cache[key]
    
    def clear(self):
        self.cache.clear()
        self.cache_stats = {'hits': 0, 'misses': 0}
    
    def get_stats(self) -> Dict:
        total = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_rate = self.cache_stats['hits'] / total if total > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.cache_stats['hits'],
            'misses': self.cache_stats['misses'],
            'hit_rate': f"{hit_rate:.1%}",
            'memory_usage': len(pickle.dumps(self.cache)) / 1024 / 1024
        }

# ============================================
# 5. МОДЕЛЬ ДАННЫХ OLAP
# ============================================
class OLAPDimension:
    def __init__(self, name: str, column: str, hierarchy: List[str] = None, description: str = ""):
        self.name = name
        self.column = column
        self.hierarchy = hierarchy or []
        self.description = description
        self.attributes = {}
        
    def add_attribute(self, name: str, column: str):
        self.attributes[name] = column
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'column': self.column,
            'hierarchy': self.hierarchy,
            'description': self.description,
            'attributes': self.attributes
        }
        
class OLAPMeasure:
    def __init__(self, name: str, column: str, default_agg: str = 'SUM', description: str = "", format: str = ""):
        self.name = name
        self.column = column
        self.default_agg = default_agg
        self.description = description
        self.format = format
        self.allowed_aggs = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT', 'MEDIAN', 'STDDEV']
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'column': self.column,
            'default_agg': self.default_agg,
            'description': self.description,
            'format': self.format
        }
        
class OLAPCube:
    def __init__(self, name: str, table_name: str, description: str = ""):
        self.name = name
        self.table_name = table_name
        self.description = description
        self.dimensions: Dict[str, OLAPDimension] = {}
        self.measures: Dict[str, OLAPMeasure] = {}
        self.calculated_members = {}
        self.partitions = []
        self.indexes = []
        
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
            'dimensions': {name: dim.to_dict() for name, dim in self.dimensions.items()},
            'measures': {name: m.to_dict() for name, m in self.measures.items()},
            'calculated_members': self.calculated_members
        }

# ============================================
# 6. OLAP МЕНЕДЖЕР
# ============================================
class OLAPManager:
    def __init__(self, conn):
        self.conn = conn
        self.cubes: Dict[str, OLAPCube] = {}
        self.query_cache = QueryCache()
        
    def create_cube_from_dataframe(self, name: str, df: pd.DataFrame, 
                                  description: str = "",
                                  auto_detect: bool = True,
                                  partition_by: str = None) -> Optional[OLAPCube]:
        """Создание куба из DataFrame"""
        try:
            table_name = f"cube_{name.lower().replace(' ', '_')}"
            
            if partition_by and partition_by in df.columns:
                for value in df[partition_by].unique():
                    partition_df = df[df[partition_by] == value]
                    partition_name = f"{table_name}_{abs(hash(str(value)))}"
                    self.conn.register('temp_partition', partition_df)
                    self.conn.execute(f"CREATE TABLE IF NOT EXISTS {partition_name} AS SELECT * FROM temp_partition")
                    
                    max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM table_partitions").fetchone()[0]
                    self.conn.execute("""
                        INSERT INTO table_partitions (id, table_name, partition_column, partition_value, row_count)
                        VALUES (?, ?, ?, ?, ?)
                    """, [max_id + 1, table_name, partition_by, str(value), len(partition_df)])
            else:
                self.conn.register('temp_df', df)
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
            
            # Создаём индексы
            for col in df.columns:
                if df[col].nunique() < 1000:
                    try:
                        self.conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col} ON {table_name}({col})")
                    except:
                        pass
            
            cube = OLAPCube(name, table_name, description)
            
            if auto_detect:
                for col in df.columns:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        measure = OLAPMeasure(col, col, 'SUM', f"Сумма {col}")
                        cube.add_measure(measure)
                    elif pd.api.types.is_datetime64_any_dtype(df[col]):
                        dim = OLAPDimension(col, col, ['Year', 'Quarter', 'Month', 'Day'], f"Дата {col}")
                        cube.add_dimension(dim)
                    else:
                        unique_count = df[col].nunique()
                        if unique_count < 100:
                            dim = OLAPDimension(col, col, description=f"Категория {col}")
                            cube.add_dimension(dim)
            
            self.cubes[name] = cube
            self._save_cube_metadata(cube)
            
            return cube
        except Exception as e:
            st.error(f"Ошибка создания куба: {e}")
            return None
    
    def _save_cube_metadata(self, cube: OLAPCube):
        """Сохранение метаданных куба"""
        definition = cube.to_dict()
        current_user = st.session_state.get('username', 'admin')
        
        exists = self.conn.execute("SELECT COUNT(*) FROM olap_cubes WHERE name = ?", [cube.name]).fetchone()[0]
        
        if exists > 0:
            self.conn.execute("""
                UPDATE olap_cubes 
                SET table_name = ?, definition = ?, updated_at = CURRENT_TIMESTAMP, owner = ?
                WHERE name = ?
            """, [cube.table_name, json.dumps(definition), current_user, cube.name])
        else:
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_cubes").fetchone()[0]
            self.conn.execute("""
                INSERT INTO olap_cubes (id, name, table_name, definition, owner)
                VALUES (?, ?, ?, ?, ?)
            """, [max_id + 1, cube.name, cube.table_name, json.dumps(definition), current_user])
    
    def load_cube(self, name: str) -> Optional[OLAPCube]:
        """Загрузка куба из БД"""
        try:
            result = self.conn.execute(
                "SELECT definition, table_name, description FROM olap_cubes WHERE name = ?",
                [name]
            ).fetchone()
            
            if result:
                definition = json.loads(result[0])
                cube = OLAPCube(name, result[1], result[2] or "")
                
                for dim_name, dim_data in definition.get('dimensions', {}).items():
                    dim = OLAPDimension(
                        dim_data.get('name', dim_name),
                        dim_data['column'],
                        dim_data.get('hierarchy', []),
                        dim_data.get('description', '')
                    )
                    cube.add_dimension(dim)
                
                for measure_name, measure_data in definition.get('measures', {}).items():
                    measure = OLAPMeasure(
                        measure_data.get('name', measure_name),
                        measure_data['column'],
                        measure_data.get('default_agg', 'SUM'),
                        measure_data.get('description', ''),
                        measure_data.get('format', '')
                    )
                    cube.add_measure(measure)
                
                cube.calculated_members = definition.get('calculated_members', {})
                self.cubes[name] = cube
                return cube
        except Exception as e:
            st.error(f"Ошибка загрузки куба: {e}")
        return None
    
    def slice_dice(self, cube_name: str, rows: List[str], cols: List[str], 
                   measures: List[str], filters: Dict = None) -> pd.DataFrame:
        """Операция slice and dice"""
        if cube_name not in self.cubes:
            st.error(f"Куб '{cube_name}' не загружен")
            return pd.DataFrame()
        
        cube = self.cubes[cube_name]
        dimensions = list(set(rows + cols))
        
        valid_measures = [m for m in measures if m in cube.measures]
        if not valid_measures:
            st.warning("Нет доступных мер")
            return pd.DataFrame()
        
        measures_with_agg = [(m, cube.measures[m].default_agg) for m in valid_measures]
        df = self.query_cube(cube_name, dimensions, measures_with_agg, filters)
        
        if df.empty:
            return df
        
        try:
            if rows and cols:
                return df.pivot_table(index=rows, columns=cols, values=valid_measures, aggfunc='sum', fill_value=0)
            elif rows:
                return df.groupby(rows)[valid_measures].sum().reset_index()
            elif cols:
                return df.groupby(cols)[valid_measures].sum().reset_index()
            else:
                return df[valid_measures].sum().to_frame().T
        except Exception as e:
            st.error(f"Ошибка сводной таблицы: {e}")
            return df
    
    def query_cube(self, cube_name: str, dimensions: List[str], 
                   measures: List[Tuple[str, str]], filters: Dict = None,
                   top_n: int = None, order_by: List[Tuple[str, str]] = None,
                   use_cache: bool = True) -> pd.DataFrame:
        """Выполнение запроса к кубу"""
        start_time = datetime.now()
        cache_key = f"{cube_name}_{dimensions}_{measures}_{filters}_{top_n}_{order_by}"
        
        if use_cache:
            cached = self.query_cache.get(cache_key)
            if cached is not None:
                return cached
        
        if cube_name not in self.cubes:
            return pd.DataFrame()
        
        cube = self.cubes[cube_name]
        table_name = cube.table_name
        
        select_parts = []
        group_by_parts = []
        
        for dim_name in dimensions:
            if dim_name in cube.dimensions:
                dim = cube.dimensions[dim_name]
                select_parts.append(f'"{dim.column}" as "{dim_name}"')
                group_by_parts.append(f'"{dim.column}"')
        
        for measure_name, agg_func in measures:
            if measure_name in cube.measures:
                measure = cube.measures[measure_name]
                agg_func = agg_func or measure.default_agg
                
                if agg_func == 'COUNT_DISTINCT':
                    select_parts.append(f'COUNT(DISTINCT "{measure.column}") as "{measure_name}"')
                elif agg_func == 'MEDIAN':
                    select_parts.append(f'MEDIAN("{measure.column}") as "{measure_name}"')
                elif agg_func == 'STDDEV':
                    select_parts.append(f'STDDEV("{measure.column}") as "{measure_name}"')
                else:
                    select_parts.append(f'{agg_func}("{measure.column}") as "{measure_name}"')
        
        if not select_parts:
            return pd.DataFrame()
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        if filters:
            where_conditions = []
            for col, value in filters.items():
                if isinstance(value, list) and value:
                    values_str = ', '.join([f"'{v}'" for v in value])
                    where_conditions.append(f'"{col}" IN ({values_str})')
                elif isinstance(value, dict):
                    if 'min' in value:
                        where_conditions.append(f'"{col}" >= {value["min"]}')
                    if 'max' in value:
                        where_conditions.append(f'"{col}" <= {value["max"]}')
                elif value is not None:
                    where_conditions.append(f'"{col}" = \'{value}\'')
            if where_conditions:
                query += f" WHERE {' AND '.join(where_conditions)}"
        
        if group_by_parts:
            query += f" GROUP BY {', '.join(group_by_parts)}"
        
        if order_by:
            order_parts = [f'"{col}" {direction}' for col, direction in order_by]
            query += f" ORDER BY {', '.join(order_parts)}"
        
        if top_n:
            query += f" LIMIT {top_n}"
        
        try:
            result = self.conn.execute(query).fetchdf()
            execution_time = (datetime.now() - start_time).total_seconds()
            self._log_query(cube_name, query, execution_time, len(result), 'SUCCESS')
            
            if use_cache and not result.empty:
                self.query_cache.set(cache_key, result)
            
            return result
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self._log_query(cube_name, query, execution_time, 0, f'ERROR: {str(e)[:100]}')
            st.error(f"Ошибка запроса: {e}")
            return pd.DataFrame()
    
    def _log_query(self, cube_name: str, query: str, execution_time: float, rows: int, status: str):
        """Логирование запроса"""
        try:
            current_user = st.session_state.get('username', 'anonymous')
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM query_history").fetchone()[0]
            self.conn.execute("""
                INSERT INTO query_history (id, cube_name, query_text, execution_time, rows_returned, user_name, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [max_id + 1, cube_name, query[:1000], execution_time, rows, current_user, status])
        except:
            pass
    
    def get_cubes_list(self) -> pd.DataFrame:
        """Список всех кубов"""
        try:
            return self.conn.execute("""
                SELECT name, description, created_at, updated_at, owner, is_public
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
                self.conn.execute(f"DROP TABLE IF EXISTS {cube.table_name}")
            self.conn.execute("DELETE FROM olap_cubes WHERE name = ?", [name])
            self.conn.execute("DELETE FROM olap_slices WHERE cube_name = ?", [name])
            if name in self.cubes:
                del self.cubes[name]
            return True
        except:
            return False

# ============================================
# 7. СИСТЕМА ПОЛЬЗОВАТЕЛЕЙ
# ============================================
class UserManager:
    def __init__(self, conn):
        self.conn = conn
    
    def authenticate(self, username: str, password: str) -> bool:
        """Аутентификация"""
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            result = self.conn.execute("""
                SELECT role FROM users 
                WHERE username = ? AND password_hash = ? AND is_active = TRUE
            """, [username, password_hash]).fetchone()
            
            if result:
                self.conn.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE username = ?", [username])
                st.session_state.username = username
                st.session_state.role = result[0]
                return True
        except:
            pass
        return False
    
    def create_user(self, username: str, password: str, role: str = 'VIEWER', email: str = "") -> bool:
        """Создание пользователя"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            exists = self.conn.execute("SELECT COUNT(*) FROM users WHERE username = ?", [username]).fetchone()[0]
            if exists > 0:
                return False
            
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
            self.conn.execute("""
                INSERT INTO users (id, username, password_hash, role, email)
                VALUES (?, ?, ?, ?, ?)
            """, [max_id + 1, username, password_hash, role, email])
            return True
        except:
            return False
    
    def update_user(self, username: str, role: str = None, email: str = None, is_active: bool = None) -> bool:
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
            if is_active is not None:
                updates.append("is_active = ?")
                params.append(is_active)
            
            if updates:
                params.append(username)
                self.conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE username = ?", params)
            return True
        except:
            return False
    
    def delete_user(self, username: str) -> bool:
        """Удаление пользователя"""
        if st.session_state.get('role') != 'ADMIN' or username == 'admin':
            return False
        
        try:
            self.conn.execute("DELETE FROM users WHERE username = ?", [username])
            return True
        except:
            return False
    
    def check_permission(self, cube_name: str, required_level: str = 'READ') -> bool:
        """Проверка прав"""
        if 'username' not in st.session_state:
            return False
        
        role = st.session_state.get('role', 'VIEWER')
        if role == 'ADMIN':
            return True
        
        try:
            result = self.conn.execute("""
                SELECT access_level FROM permissions 
                WHERE user_role = ? AND (cube_name = ? OR cube_name = '*')
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
                SELECT username, role, email, created_at, last_login, is_active
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
            
            if exists > 0:
                self.conn.execute(
                    "UPDATE permissions SET access_level = ?, granted_by = ?, granted_at = CURRENT_TIMESTAMP WHERE user_role = ? AND cube_name = ?",
                    [access_level, st.session_state.get('username'), role, cube_name]
                )
            else:
                max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
                self.conn.execute(
                    "INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by) VALUES (?, ?, ?, ?, ?)",
                    [max_id + 1, role, cube_name, access_level, st.session_state.get('username')]
                )
            return True
        except:
            return False
    
    def revoke_permission(self, role: str, cube_name: str) -> bool:
        """Отзыв прав"""
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        try:
            self.conn.execute("DELETE FROM permissions WHERE user_role = ? AND cube_name = ?", [role, cube_name])
            return True
        except:
            return False

# ============================================
# 8. ДАШБОРДЫ И ВИЗУАЛИЗАЦИИ
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
    
    def create_bar_chart(self, cube_name: str, dimension: str, measure: str, top_n: int = 10):
        """Столбчатая диаграмма"""
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            top_n=top_n, order_by=[(measure, 'DESC')]
        )
        
        if df.empty:
            return None
        
        fig = px.bar(
            df, x=dimension, y=measure,
            title=f"{measure} по {dimension}",
            color=measure, color_continuous_scale='Blues'
        )
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
        
        fig = px.line(
            df, x=date_dim, y=measure,
            title=f"Динамика {measure}"
        )
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
        
        fig = px.pie(
            df, values=measure, names=dimension,
            title=f"Доля {measure} по {dimension}"
        )
        fig.update_layout(height=500)
        return fig
    
    def create_heatmap(self, cube_name: str, row_dim: str, col_dim: str, measure: str):
        """Тепловая карта"""
        pivot_df = self.olap_manager.slice_dice(cube_name, [row_dim], [col_dim], [measure])
        
        if pivot_df.empty:
            return None
        
        fig = px.imshow(
            pivot_df,
            title=f"Heatmap: {measure}",
            color_continuous_scale='RdBu_r',
            aspect='auto'
        )
        fig.update_layout(height=500)
        return fig
    
    def create_scatter_plot(self, cube_name: str, x_measure: str, y_measure: str, color_dim: str = None):
        """Диаграмма рассеяния"""
        dims = [color_dim] if color_dim else []
        measures = [(x_measure, 'SUM'), (y_measure, 'SUM')]
        
        df = self.olap_manager.query_cube(cube_name, dims, measures)
        
        if df.empty:
            return None
        
        if color_dim:
            fig = px.scatter(
                df, x=x_measure, y=y_measure, color=color_dim,
                title=f"Корреляция {x_measure} и {y_measure}"
            )
        else:
            fig = px.scatter(
                df, x=x_measure, y=y_measure,
                title=f"Корреляция {x_measure} и {y_measure}"
            )
        fig.update_layout(height=500)
        return fig
    
    def create_kpi_cards(self, cube_name: str, measures: List[str]) -> Dict:
        """KPI карточки"""
        kpis = {}
        for measure in measures:
            df = self.olap_manager.query_cube(cube_name, [], [(measure, 'SUM')])
            current = df[measure].iloc[0] if not df.empty else 0
            kpis[measure] = {'current': current, 'change': 0, 'change_pct': 0}
        return kpis
    
    def save_dashboard(self, name: str, cube_name: str, config: Dict) -> bool:
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
                    SET config = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE name = ? AND cube_name = ?
                """, [json.dumps(config), name, cube_name])
            else:
                max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM dashboards").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO dashboards (id, name, cube_name, config, owner)
                    VALUES (?, ?, ?, ?, ?)
                """, [max_id + 1, name, cube_name, json.dumps(config), current_user])
            return True
        except:
            return False
    
    def load_dashboards(self, cube_name: str = None) -> pd.DataFrame:
        """Загрузка дашбордов"""
        try:
            if cube_name:
                return self.conn.execute("""
                    SELECT id, name, cube_name, created_at, updated_at, owner
                    FROM dashboards
                    WHERE cube_name = ?
                    ORDER BY updated_at DESC
                """, [cube_name]).fetchdf()
            else:
                return self.conn.execute("""
                    SELECT id, name, cube_name, created_at, updated_at, owner
                    FROM dashboards
                    ORDER BY updated_at DESC
                """).fetchdf()
        except:
            return pd.DataFrame()

# ============================================
# 9. API ДЛЯ ВНЕШНИХ СИСТЕМ
# ============================================
class OLAPAPI:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
    
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
                'success': True,
                'data': df.to_dict('records'),
                'row_count': len(df),
                'columns': df.columns.tolist()
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def export_data(self, cube_name: str, format: str = 'csv', query_config: Dict = None) -> bytes:
        """Экспорт данных"""
        if query_config:
            dimensions = query_config.get('dimensions', [])
            measures = [(m, 'SUM') for m in query_config.get('measures', [])]
            df = self.olap_manager.query_cube(cube_name, dimensions, measures)
        else:
            cube = self.olap_manager.cubes.get(cube_name)
            if not cube:
                return b''
            dimensions = list(cube.dimensions.keys())
            measures = [(m, 'SUM') for m in cube.measures.keys()]
            df = self.olap_manager.query_cube(cube_name, dimensions, measures)
        
        output = io.BytesIO()
        
        if format == 'csv':
            df.to_csv(output, index=False)
        elif format == 'excel':
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
        elif format == 'json':
            output.write(df.to_json(orient='records').encode())
        elif format == 'parquet':
            df.to_parquet(output)
        
        return output.getvalue()
    
    def get_cube_metadata(self, cube_name: str) -> Dict:
        """Метаданные куба"""
        cube = self.olap_manager.cubes.get(cube_name)
        if not cube:
            return {'error': 'Cube not found'}
        
        return {
            'name': cube.name,
            'description': cube.description,
            'dimensions': [
                {'name': name, 'column': dim.column, 'hierarchy': dim.hierarchy}
                for name, dim in cube.dimensions.items()
            ],
            'measures': [
                {'name': name, 'column': m.column, 'default_agg': m.default_agg}
                for name, m in cube.measures.items()
            ]
        }
    
    def get_api_docs(self) -> Dict:
        """Документация API"""
        return {
            'version': '1.0',
            'endpoints': {
                '/api/query': {
                    'method': 'POST',
                    'description': 'Execute OLAP query',
                    'body': {
                        'cube': 'string',
                        'dimensions': 'array',
                        'measures': 'array',
                        'filters': 'object',
                        'aggregations': 'object',
                        'top_n': 'int',
                        'order_by': 'array'
                    }
                },
                '/api/export': {
                    'method': 'POST',
                    'description': 'Export data',
                    'body': {
                        'cube': 'string',
                        'format': 'csv|excel|json|parquet',
                        'query': 'object'
                    }
                },
                '/api/metadata/{cube}': {
                    'method': 'GET',
                    'description': 'Get cube metadata'
                }
            }
        }

# ============================================
# 10. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
class OLAPInterface:
    def __init__(self):
        self.conn = get_connection()
        self.olap_manager = OLAPManager(self.conn)
        self.user_manager = UserManager(self.conn)
        self.dashboard_manager = DashboardManager(self.olap_manager)
        self.api = OLAPAPI(self.olap_manager)
        
        # Инициализация session_state
        defaults = {
            'current_cube': None,
            'authenticated': False,
            'drill_path': [],
            'filters': {},
            'pivot_rows': [],
            'pivot_cols': [],
            'pivot_measures': [],
            'selected_dimensions': [],
            'selected_measures': []
        }
        
        for key, default in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default
    
    def run(self):
        """Запуск приложения"""
        if not st.session_state.authenticated:
            self.render_login_page()
        else:
            self.render_main_interface()
    
    def render_login_page(self):
        """Страница входа"""
        st.markdown("<div class='login-container'>", unsafe_allow_html=True)
        st.markdown("## 🔐 OLAP Analytics Pro")
        st.markdown("---")
        
        username = st.text_input("👤 Логин", placeholder="admin")
        password = st.text_input("🔑 Пароль", type="password", placeholder="admin123")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🚪 Войти", type="primary", use_container_width=True):
                if self.user_manager.authenticate(username, password):
                    st.session_state.authenticated = True
                    st.success("✅ Успешный вход!")
                    st.rerun()
                else:
                    st.error("❌ Неверный логин или пароль")
        
        st.markdown("---")
        st.markdown("<p style='text-align: center; color: #666;'>admin / admin123</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    def render_main_interface(self):
        """Основной интерфейс"""
        st.markdown(f"""
        <div class='main-header'>
            <h1>🎲 OLAP Analytics Platform</h1>
            <p>{st.session_state.username} <span class='user-role-badge'>{st.session_state.role}</span></p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.sidebar:
            st.markdown("## 🎯 Навигация")
            
            mode = st.radio("Режим", [
                "📊 Анализ", "📈 Дашборды", "🏗️ Конструктор", 
                "💾 Срезы", "⚙️ Администрирование", "🔌 API"
            ])
            
            if st.button("🚪 Выход", use_container_width=True):
                for key in ['authenticated', 'username', 'role', 'current_cube']:
                    st.session_state.pop(key, None)
                st.rerun()
            
            st.markdown("---")
            self.render_sidebar_cubes()
            st.markdown("---")
            self.render_sidebar_stats()
        
        # Рендеринг выбранного режима
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
            selected = st.selectbox("Выберите куб", cube_names)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🎲 Загрузить", use_container_width=True):
                    if self.user_manager.check_permission(selected, 'READ'):
                        cube = self.olap_manager.load_cube(selected)
                        if cube:
                            st.session_state.current_cube = cube
                            st.success(f"✅ '{selected}' загружен")
                            st.rerun()
                    else:
                        st.error("❌ Нет доступа")
            
            with col2:
                if st.button("🔄 Обновить", use_container_width=True):
                    st.rerun()
        else:
            st.info("Нет кубов. Создайте в Конструкторе.")
    
    def render_sidebar_stats(self):
        """Статистика в боковой панели"""
        with st.expander("📊 Статистика"):
            stats = self.olap_manager.query_cache.get_stats()
            st.metric("Кэш", f"{stats['size']} запросов")
            st.metric("Hit Rate", stats['hit_rate'])
            
            if st.button("🗑️ Очистить кэш", use_container_width=True):
                self.olap_manager.query_cache.clear()
                st.success("Кэш очищен")
                st.rerun()
    
    def render_analysis_mode(self):
        """Режим анализа"""
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню")
            return
        
        cube = st.session_state.current_cube
        
        tabs = st.tabs(["🎯 Сводная", "📊 Графики", "🔍 Drill-down", "📋 Данные", "⚡ Оптимизация"])
        
        with tabs[0]:
            self.render_pivot_table(cube)
        
        with tabs[1]:
            self.render_charts(cube)
        
        with tabs[2]:
            self.render_drill_down(cube)
        
        with tabs[3]:
            self.render_raw_data(cube)
        
        with tabs[4]:
            self.render_optimization(cube)
    
    def render_pivot_table(self, cube: OLAPCube):
        """Сводная таблица"""
        st.markdown("### 🎯 Интерактивная сводная таблица")
        
        if not cube.dimensions or not cube.measures:
            st.warning("В кубе нет измерений или мер")
            return
        
        col1, col2 = st.columns(2)
        with col1:
            rows = st.multiselect("Строки", list(cube.dimensions.keys()), key="pivot_rows")
        with col2:
            cols = st.multiselect("Колонки", list(cube.dimensions.keys()), key="pivot_cols")
        
        measures = st.multiselect("Меры", list(cube.measures.keys()), key="pivot_measures")
        
        with st.expander("⚙️ Настройки"):
            c1, c2, c3 = st.columns(3)
            with c1:
                top_n = st.number_input("Топ N", 0, 10000, 0)
            with c2:
                export_format = st.selectbox("Экспорт", ["CSV", "Excel", "JSON"])
            with c3:
                use_cache = st.checkbox("Кэш", True)
        
        if st.button("🎯 Построить", type="primary") and measures:
            with st.spinner("Выполнение..."):
                df = self.olap_manager.slice_dice(cube.name, rows, cols, measures, st.session_state.filters)
                
                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=500)
                    
                    # Экспорт
                    if export_format == "CSV":
                        st.download_button("📥 CSV", df.to_csv(), "pivot.csv")
                    elif export_format == "Excel":
                        buf = io.BytesIO()
                        df.to_excel(buf, sheet_name='Pivot')
                        st.download_button("📥 Excel", buf.getvalue(), "pivot.xlsx")
                    elif export_format == "JSON":
                        st.download_button("📥 JSON", df.to_json(orient='records'), "pivot.json")
                else:
                    st.info("Нет данных")
    
    def render_charts(self, cube: OLAPCube):
        """Графики"""
        st.markdown("### 📊 Визуализации")
        
        chart_type = st.selectbox("Тип", ["Treemap", "Bar Chart", "Line Chart", "Pie Chart", "Heatmap", "Scatter"])
        
        if chart_type == "Treemap":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Элементов", 5, 50, 20)
            
            if st.button("Создать"):
                fig = self.dashboard_manager.create_treemap(cube.name, dim, measure, top_n)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Bar Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Элементов", 5, 50, 10)
            
            if st.button("Создать"):
                fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, top_n)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Line Chart":
            date_dims = [d for d, dim in cube.dimensions.items() if dim.hierarchy]
            if date_dims:
                dim = st.selectbox("Измерение даты", date_dims)
                measure = st.selectbox("Мера", list(cube.measures.keys()))
                
                if st.button("Создать"):
                    fig = self.dashboard_manager.create_line_chart(cube.name, dim, measure)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет измерений с иерархией дат")
        
        elif chart_type == "Pie Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Элементов", 3, 20, 8)
            
            if st.button("Создать"):
                fig = self.dashboard_manager.create_pie_chart(cube.name, dim, measure, top_n)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Heatmap":
            if len(cube.dimensions) >= 2:
                row_dim = st.selectbox("Строки", list(cube.dimensions.keys()), key="heat_row")
                col_dim = st.selectbox("Колонки", list(cube.dimensions.keys()), key="heat_col")
                measure = st.selectbox("Мера", list(cube.measures.keys()))
                
                if st.button("Создать"):
                    fig = self.dashboard_manager.create_heatmap(cube.name, row_dim, col_dim, measure)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нужно минимум 2 измерения")
        
        elif chart_type == "Scatter":
            if len(cube.measures) >= 2:
                x_measure = st.selectbox("X", list(cube.measures.keys()), key="scatter_x")
                y_measure = st.selectbox("Y", list(cube.measures.keys()), key="scatter_y")
                color_dim = st.selectbox("Цвет (опционально)", ["Нет"] + list(cube.dimensions.keys()))
                
                if st.button("Создать"):
                    fig = self.dashboard_manager.create_scatter_plot(
                        cube.name, x_measure, y_measure,
                        color_dim if color_dim != "Нет" else None
                    )
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нужно минимум 2 меры")
    
    def render_drill_down(self, cube: OLAPCube):
        """Drill-down анализ"""
        st.markdown("### 🔍 Drill-down по иерархиям")
        
        hier_dims = {n: d for n, d in cube.dimensions.items() if d.hierarchy}
        
        if hier_dims:
            dim_name = st.selectbox("Измерение", list(hier_dims.keys()))
            dim = hier_dims[dim_name]
            
            st.markdown(f"**Иерархия:** {' → '.join(dim.hierarchy)}")
            
            level = len(st.session_state.drill_path)
            
            if level < len(dim.hierarchy):
                next_level = dim.hierarchy[level]
                
                if st.button(f"⬇️ Drill down: {next_level}"):
                    query = f"""
                        SELECT DATE_PART('{next_level.lower()}', "{dim.column}") as {next_level},
                               COUNT(*) as count
                        FROM {cube.table_name}
                    """
                    
                    if st.session_state.drill_path:
                        for i, val in enumerate(st.session_state.drill_path):
                            lvl = dim.hierarchy[i].lower()
                            query += f" WHERE DATE_PART('{lvl}', \"{dim.column}\") = '{val}'"
                    
                    query += f" GROUP BY {next_level} ORDER BY {next_level}"
                    
                    df = self.conn.execute(query).fetchdf()
                    st.dataframe(df, use_container_width=True)
                    
                    if not df.empty:
                        selected = st.selectbox(f"Выберите {next_level}", df[next_level].tolist())
                        if st.button("Продолжить"):
                            st.session_state.drill_path.append(selected)
                            st.rerun()
            
            if st.session_state.drill_path:
                if st.button("⬆️ Drill up"):
                    st.session_state.drill_path.pop()
                    st.rerun()
                if st.button("🔄 Сбросить"):
                    st.session_state.drill_path = []
                    st.rerun()
        else:
            st.info("Нет измерений с иерархиями")
    
    def render_raw_data(self, cube: OLAPCube):
        """Просмотр сырых данных"""
        st.markdown("### 📋 Данные куба")
        
        try:
            df = self.conn.execute(f"SELECT * FROM {cube.table_name} LIMIT 1000").fetchdf()
            st.dataframe(df, use_container_width=True, height=500)
            
            st.metric("Всего строк", self.conn.execute(f"SELECT COUNT(*) FROM {cube.table_name}").fetchone()[0])
        except Exception as e:
            st.error(f"Ошибка: {e}")
    
    def render_optimization(self, cube: OLAPCube):
        """Оптимизация"""
        st.markdown("### ⚡ Оптимизация")
        
        st.markdown("#### 📊 Статистика запросов")
        stats = self.conn.execute("""
            SELECT cube_name, COUNT(*) as count, AVG(execution_time) as avg_time
            FROM query_history WHERE cube_name = ? AND status = 'SUCCESS'
            GROUP BY cube_name
        """, [cube.name]).fetchdf()
        
        if not stats.empty:
            st.dataframe(stats, use_container_width=True)
        
        st.markdown("#### 🗑️ Управление")
        if st.button("Очистить кэш куба", type="secondary"):
            self.olap_manager.query_cache.clear()
            st.success("Кэш очищен")
    
    def render_dashboard_mode(self):
        """Режим дашбордов"""
        st.markdown("### 📈 Дашборды")
        
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб")
            return
        
        cube = st.session_state.current_cube
        
        # Сохранение дашборда
        with st.expander("💾 Сохранить дашборд"):
            name = st.text_input("Название")
            if st.button("Сохранить") and name:
                config = {
                    'cube': cube.name,
                    'created_by': st.session_state.username
                }
                if self.dashboard_manager.save_dashboard(name, cube.name, config):
                    st.success("✅ Сохранено")
        
        # Загрузка дашбордов
        dashboards = self.dashboard_manager.load_dashboards(cube.name)
        if not dashboards.empty:
            st.markdown("#### 📂 Сохранённые дашборды")
            st.dataframe(dashboards[['name', 'created_at', 'owner']], use_container_width=True)
        
        # KPI
        st.markdown("#### 🎯 KPI")
        measures = list(cube.measures.keys())[:4]
        
        if measures:
            cols = st.columns(len(measures))
            kpis = self.dashboard_manager.create_kpi_cards(cube.name, measures)
            
            for i, (m, v) in enumerate(kpis.items()):
                with cols[i]:
                    st.markdown(f"""
                    <div class='kpi-card'>
                        <div class='kpi-value'>{v['current']:,.0f}</div>
                        <div class='kpi-label'>{m}</div>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Графики
        st.markdown("#### 📊 Графики")
        c1, c2 = st.columns(2)
        
        with c1:
            if cube.dimensions and cube.measures:
                dim = list(cube.dimensions.keys())[0]
                measure = list(cube.measures.keys())[0]
                fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, 8)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        with c2:
            if len(cube.dimensions) > 1 and cube.measures:
                dim1 = list(cube.dimensions.keys())[0]
                dim2 = list(cube.dimensions.keys())[1]
                measure = list(cube.measures.keys())[0]
                fig = self.dashboard_manager.create_heatmap(cube.name, dim1, dim2, measure)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
    
    def render_cube_designer(self):
        """Конструктор кубов"""
        st.markdown("### 🏗️ Конструктор OLAP кубов")
        
        if not self.user_manager.check_permission('*', 'WRITE'):
            st.error("❌ Недостаточно прав")
            return
        
        tab1, tab2 = st.tabs(["📤 Создать", "📋 Управление"])
        
        with tab1:
            uploaded = st.file_uploader("Загрузите данные", type=['csv', 'xlsx', 'parquet'])
            
            if uploaded:
                try:
                    if uploaded.name.endswith('.csv'):
                        df = pd.read_csv(uploaded)
                    elif uploaded.name.endswith('.parquet'):
                        df = pd.read_parquet(uploaded)
                    else:
                        df = pd.read_excel(uploaded)
                    
                    st.markdown("**Предпросмотр:**")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        name = st.text_input("Название", f"Cube_{datetime.now().strftime('%Y%m%d')}")
                    with col2:
                        desc = st.text_input("Описание", "")
                    
                    if st.button("🎲 Создать куб", type="primary"):
                        with st.spinner("Создание..."):
                            cube = self.olap_manager.create_cube_from_dataframe(name, df, desc)
                            if cube:
                                st.session_state.current_cube = cube
                                st.success(f"✅ '{name}' создан!")
                                
                                c1, c2, c3 = st.columns(3)
                                c1.metric("Строк", len(df))
                                c2.metric("Измерений", len(cube.dimensions))
                                c3.metric("Мер", len(cube.measures))
                except Exception as e:
                    st.error(f"Ошибка: {e}")
        
        with tab2:
            cubes = self.olap_manager.get_cubes_list()
            if not cubes.empty:
                st.dataframe(cubes, use_container_width=True)
                
                st.markdown("---")
                st.markdown("### 🗑️ Удаление")
                to_delete = st.selectbox("Выберите куб для удаления", cubes['name'].tolist())
                
                if st.button("🗑️ Удалить", type="secondary"):
                    if self.olap_manager.delete_cube(to_delete):
                        st.success(f"✅ '{to_delete}' удалён")
                        st.rerun()
                    else:
                        st.error("Ошибка удаления")
            else:
                st.info("Нет созданных кубов")
    
    def render_slice_manager(self):
        """Управление срезами"""
        st.markdown("### 💾 Срезы данных")
        
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб")
            return
        
        cube = st.session_state.current_cube
        
        # Сохранение
        st.markdown("#### 💾 Сохранить срез")
        name = st.text_input("Название")
        desc = st.text_area("Описание")
        
        if st.button("Сохранить") and name:
            slice_def = {
                'cube': cube.name,
                'filters': st.session_state.filters,
                'dimensions': st.session_state.pivot_rows + st.session_state.pivot_cols,
                'measures': st.session_state.pivot_measures,
                'description': desc
            }
            
            try:
                max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_slices").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO olap_slices (id, cube_name, slice_name, definition, description, owner)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [max_id + 1, cube.name, name, json.dumps(slice_def), desc, st.session_state.username])
                st.success("✅ Сохранено")
            except Exception as e:
                st.error(f"Ошибка: {e}")
        
        # Загрузка
        st.markdown("#### 📂 Сохранённые срезы")
        slices = self.conn.execute("""
            SELECT id, slice_name, description, created_at, owner
            FROM olap_slices WHERE cube_name = ? ORDER BY created_at DESC
        """, [cube.name]).fetchdf()
        
        if not slices.empty:
            for _, row in slices.iterrows():
                with st.expander(f"{row['slice_name']} - {row['created_at']}"):
                    st.markdown(f"**Владелец:** {row['owner']}")
                    st.markdown(f"**Описание:** {row['description'] or 'Нет'}")
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("📂 Загрузить", key=f"load_{row['id']}"):
                            slice_data = json.loads(
                                self.conn.execute("SELECT definition FROM olap_slices WHERE id = ?", [row['id']]).fetchone()[0]
                            )
                            st.session_state.filters = slice_data.get('filters', {})
                            st.success("✅ Загружено")
                            st.rerun()
                    with c2:
                        if st.button("🗑️ Удалить", key=f"del_{row['id']}"):
                            self.conn.execute("DELETE FROM olap_slices WHERE id = ?", [row['id']])
                            st.rerun()
        else:
            st.info("Нет сохранённых срезов")
    
    def render_admin_panel(self):
        """Админ-панель"""
        st.markdown("### ⚙️ Администрирование")
        
        if st.session_state.role != 'ADMIN':
            st.error("❌ Только для администраторов")
            return
        
        tabs = st.tabs(["👥 Пользователи", "🔐 Права", "📊 Мониторинг", "🗄️ БД"])
        
        with tabs[0]:
            st.markdown("#### 👥 Пользователи")
            
            with st.expander("➕ Создать"):
                new_user = st.text_input("Логин")
                new_pass = st.text_input("Пароль", type="password")
                new_role = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"])
                new_email = st.text_input("Email")
                
                if st.button("Создать"):
                    if self.user_manager.create_user(new_user, new_pass, new_role, new_email):
                        st.success("✅ Создан")
                        st.rerun()
                    else:
                        st.error("Ошибка")
            
            users = self.user_manager.get_users_list()
            if not users.empty:
                st.dataframe(users, use_container_width=True)
                
                st.markdown("#### ✏️ Управление")
                selected_user = st.selectbox("Пользователь", users['username'].tolist())
                
                c1, c2, c3 = st.columns(3)
                with c1:
                    new_role_for = st.selectbox("Новая роль", ["VIEWER", "ANALYST", "ADMIN"], key="edit_role")
                with c2:
                    is_active = st.checkbox("Активен", True, key="edit_active")
                with c3:
                    if st.button("Обновить"):
                        if self.user_manager.update_user(selected_user, new_role_for, is_active=is_active):
                            st.success("✅ Обновлено")
                            st.rerun()
                
                if selected_user != 'admin':
                    if st.button("🗑️ Удалить пользователя", type="secondary"):
                        if self.user_manager.delete_user(selected_user):
                            st.success("✅ Удалён")
                            st.rerun()
        
        with tabs[1]:
            st.markdown("#### 🔐 Права доступа")
            
            cubes = self.olap_manager.get_cubes_list()
            if not cubes.empty:
                role = st.selectbox("Роль", ["VIEWER", "ANALYST"])
                cube = st.selectbox("Куб", ['*'] + cubes['name'].tolist())
                access = st.selectbox("Уровень", ["READ", "WRITE"])
                
                if st.button("Назначить"):
                    if self.user_manager.grant_permission(role, cube, access):
                        st.success("✅ Назначено")
                
                st.markdown("#### 📋 Текущие права")
                perms = self.conn.execute("""
                    SELECT user_role, cube_name, access_level, granted_by, granted_at
                    FROM permissions ORDER BY user_role, cube_name
                """).fetchdf()
                
                if not perms.empty:
                    st.dataframe(perms, use_container_width=True)
                    
                    st.markdown("#### 🗑️ Отозвать")
                    revoke_role = st.selectbox("Роль", perms['user_role'].unique(), key="revoke_role")
                    revoke_cube = st.selectbox("Куб", perms[perms['user_role'] == revoke_role]['cube_name'].tolist(), key="revoke_cube")
                    
                    if st.button("Отозвать"):
                        if self.user_manager.revoke_permission(revoke_role, revoke_cube):
                            st.success("✅ Отозвано")
                            st.rerun()
        
        with tabs[2]:
            st.markdown("#### 📊 Мониторинг")
            
            st.markdown("**Статистика запросов:**")
            stats = self.conn.execute("""
                SELECT cube_name, COUNT(*) as count, AVG(execution_time) as avg_time,
                       MAX(execution_time) as max_time, AVG(rows_returned) as avg_rows
                FROM query_history WHERE status = 'SUCCESS'
                GROUP BY cube_name ORDER BY avg_time DESC
            """).fetchdf()
            
            if not stats.empty:
                st.dataframe(stats, use_container_width=True)
            
            st.markdown("**Кэш:**")
            cache_stats = self.olap_manager.query_cache.get_stats()
            st.json(cache_stats)
            
            st.markdown("**Последние запросы:**")
            recent = self.conn.execute("""
                SELECT timestamp, user_name, cube_name, execution_time, rows_returned, status
                FROM query_history ORDER BY timestamp DESC LIMIT 20
            """).fetchdf()
            
            if not recent.empty:
                st.dataframe(recent, use_container_width=True)
        
        with tabs[3]:
            st.markdown("#### 🗄️ База данных")
            
            if st.button("📊 Список таблиц"):
                tables = self.conn.execute("SHOW TABLES").fetchdf()
                st.dataframe(tables, use_container_width=True)
            
            if st.button("🗜️ Оптимизация (VACUUM)"):
                self.conn.execute("VACUUM")
                st.success("✅ Оптимизировано")
            
            if st.button("📈 Размер БД"):
                size = os.path.getsize('olap_analytics.db') / 1024 / 1024
                st.metric("Размер", f"{size:.2f} MB")
    
    def render_api_documentation(self):
        """API документация"""
        st.markdown("### 🔌 API")
        
        docs = self.api.get_api_docs()
        
        st.markdown(f"**Версия:** {docs['version']}")
        
        for endpoint, info in docs['endpoints'].items():
            with st.expander(f"{info['method']} {endpoint}"):
                st.markdown(f"**{info['description']}**")
                st.markdown("**Параметры:**")
                st.json(info.get('body', {}))
        
        st.markdown("---")
        st.markdown("#### 🧪 Тестирование")
        
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            
            st.markdown("**Метаданные куба:**")
            meta = self.api.get_cube_metadata(cube.name)
            st.json(meta)
            
            st.markdown("**Экспорт:**")
            fmt = st.selectbox("Формат", ["csv", "excel", "json", "parquet"])
            
            if st.button("📥 Экспортировать"):
                data = self.api.export_data(cube.name, fmt)
                if data:
                    st.download_button(
                        "Скачать", data, f"{cube.name}.{fmt}",
                        "application/octet-stream"
                    )

# ============================================
# 11. ЗАПУСК
# ============================================
def main():
    interface = OLAPInterface()
    interface.run()

if __name__ == "__main__":
    main()
