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
# 2. ПРИНУДИТЕЛЬНОЕ УДАЛЕНИЕ СТАРОЙ БД
# ============================================
DB_PATH = 'olap_analytics.db'

if 'db_cleaned' not in st.session_state:
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
            st.sidebar.success("🗑️ Старая БД удалена, создаётся новая...")
        except Exception as e:
            st.sidebar.error(f"Ошибка удаления БД: {e}")
    st.session_state.db_cleaned = True

# ============================================
# 3. ПОДКЛЮЧЕНИЕ К БД
# ============================================
@st.cache_resource
def get_connection():
    conn = duckdb.connect(DB_PATH)
    conn.execute("INSTALL json; LOAD json;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    return conn

conn = get_connection()

def init_database():
    """Полная инициализация базы данных с правильной структурой"""
    try:
        # ============================================
        # ТАБЛИЦА ПОЛЬЗОВАТЕЛЕЙ
        # ============================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username VARCHAR NOT NULL UNIQUE,
                password_hash VARCHAR NOT NULL,
                role VARCHAR DEFAULT 'VIEWER',
                email VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # ============================================
        # СОЗДАНИЕ АДМИНА (ГАРАНТИРОВАННО)
        # ============================================
        admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
        
        # Удаляем старого админа если есть
        conn.execute("DELETE FROM users WHERE username = 'admin'")
        
        # Создаём нового админа
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
        conn.execute("""
            INSERT INTO users (id, username, password_hash, role, email, is_active)
            VALUES (?, 'admin', ?, 'ADMIN', 'admin@olap.local', TRUE)
        """, [max_id + 1, admin_hash])
        
        # Проверяем создание
        check = conn.execute("SELECT username, role, is_active FROM users WHERE username = 'admin'").fetchone()
        if check:
            st.sidebar.success(f"✅ Админ: admin / admin123 (роль: {check[1]})")
        
        # ============================================
        # ТАБЛИЦА ПРАВ ДОСТУПА
        # ============================================
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
        
        conn.execute("DELETE FROM permissions WHERE user_role = 'ADMIN' AND cube_name = '*'")
        max_perm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
        conn.execute("""
            INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by)
            VALUES (?, 'ADMIN', '*', 'ADMIN', 'system')
        """, [max_perm + 1])
        
        # ============================================
        # ТАБЛИЦА КУБОВ
        # ============================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olap_cubes (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL UNIQUE,
                table_name VARCHAR NOT NULL,
                definition JSON,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                owner VARCHAR,
                is_public BOOLEAN DEFAULT FALSE
            )
        """)
        
        # ============================================
        # ТАБЛИЦА СРЕЗОВ
        # ============================================
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
        
        # ============================================
        # ТАБЛИЦА ИСТОРИИ ЗАПРОСОВ
        # ============================================
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
        
        # ============================================
        # ТАБЛИЦА ПАРТИЦИЙ
        # ============================================
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
        
        # ============================================
        # ТАБЛИЦА ДАШБОРДОВ
        # ============================================
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
        
        # ============================================
        # ТАБЛИЦА ОТЧЁТОВ
        # ============================================
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
        
        # ============================================
        # ТАБЛИЦА АУДИТА
        # ============================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY,
                user_name VARCHAR,
                action VARCHAR,
                details JSON,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        return True
        
    except Exception as e:
        st.error(f"Ошибка инициализации БД: {e}")
        return False

init_database()

# ============================================
# 4. ФУНКЦИЯ АУДИТА
# ============================================
def log_audit(action: str, details: Dict = None):
    """Логирование действий пользователя"""
    try:
        username = st.session_state.get('username', 'system')
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM audit_log").fetchone()[0]
        conn.execute("""
            INSERT INTO audit_log (id, user_name, action, details)
            VALUES (?, ?, ?, ?)
        """, [max_id + 1, username, action, json.dumps(details or {})])
    except:
        pass

# ============================================
# 5. CSS СТИЛИ
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
    
    .hierarchy-level {
        margin-left: 20px;
        padding: 3px;
        border-left: 2px solid #ddd;
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
    
    .drill-indicator {
        background: #fff3e0;
        border-left: 4px solid #ff9800;
        padding: 10px;
        margin: 10px 0;
        border-radius: 4px;
    }
    
    .user-role-badge {
        background: #4caf50;
        color: white;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.8em;
    }
    
    .api-endpoint {
        background: #f5f5f5;
        padding: 10px;
        border-radius: 5px;
        font-family: monospace;
        margin: 5px 0;
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
    
    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    
    .metric-value {
        font-size: 2em;
        font-weight: bold;
        color: #1e3c72;
    }
    
    .metric-label {
        color: #666;
        font-size: 0.9em;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 6. КЭШИРОВАНИЕ ЗАПРОСОВ
# ============================================
class QueryCache:
    """Система кэширования запросов для ускорения работы"""
    
    def __init__(self):
        self.cache = {}
        self.cache_stats = {'hits': 0, 'misses': 0}
        
    def get_cache_key(self, query: str, params: tuple = ()) -> str:
        content = query + str(params)
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, query: str, params: tuple = ()) -> Optional[pd.DataFrame]:
        key = self.get_cache_key(query, params)
        if key in self.cache:
            self.cache_stats['hits'] += 1
            return self.cache[key]['data'].copy()
        self.cache_stats['misses'] += 1
        return None
    
    def set(self, query: str, data: pd.DataFrame, params: tuple = (), ttl: int = 3600):
        key = self.get_cache_key(query, params)
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
# 7. МОДЕЛЬ ДАННЫХ OLAP
# ============================================
class OLAPDimension:
    """Измерение OLAP с поддержкой иерархий"""
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
    """Мера OLAP с поддержкой разных агрегаций"""
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
    """OLAP Куб для многомерного анализа"""
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
# 8. OLAP МЕНЕДЖЕР
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
            
            log_audit("CREATE_CUBE", {"cube": name, "rows": len(df), "columns": len(df.columns)})
            
            return cube
        except Exception as e:
            st.error(f"Ошибка создания куба: {e}")
            return None
    
    def _save_cube_metadata(self, cube: OLAPCube):
        definition = cube.to_dict()
        current_user = st.session_state.get('username', 'admin')
        
        exists = self.conn.execute("SELECT COUNT(*) FROM olap_cubes WHERE name = ?", [cube.name]).fetchone()[0]
        
        if exists > 0:
            self.conn.execute("""
                UPDATE olap_cubes 
                SET table_name = ?, definition = ?, updated_at = CURRENT_TIMESTAMP, owner = ?, description = ?
                WHERE name = ?
            """, [cube.table_name, json.dumps(definition), current_user, cube.description, cube.name])
        else:
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_cubes").fetchone()[0]
            self.conn.execute("""
                INSERT INTO olap_cubes (id, name, table_name, definition, description, owner)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [max_id + 1, cube.name, cube.table_name, json.dumps(definition), cube.description, current_user])
    
    def load_cube(self, name: str) -> Optional[OLAPCube]:
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
                    for attr_name, attr_col in dim_data.get('attributes', {}).items():
                        dim.add_attribute(attr_name, attr_col)
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
                
                log_audit("LOAD_CUBE", {"cube": name})
                
                return cube
        except Exception as e:
            st.error(f"Ошибка загрузки куба: {e}")
        return None
    
    def slice_dice(self, cube_name: str, rows: List[str], cols: List[str], 
                   measures: List[str], filters: Dict = None) -> pd.DataFrame:
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
    
    def query_cube(self, cube_name: str, 
                   dimensions: List[str], 
                   measures: List[Tuple[str, str]],
                   filters: Dict[str, Any] = None,
                   top_n: int = None,
                   order_by: List[Tuple[str, str]] = None,
                   use_cache: bool = True) -> pd.DataFrame:
        
        start_time = datetime.now()
        
        cache_key = f"{cube_name}_{dimensions}_{measures}_{filters}_{top_n}_{order_by}"
        if use_cache:
            cached_result = self.query_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
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
        try:
            current_user = st.session_state.get('username', 'anonymous')
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM query_history").fetchone()[0]
            self.conn.execute("""
                INSERT INTO query_history (id, cube_name, query_text, execution_time, rows_returned, user_name, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [max_id + 1, cube_name, query[:1000], execution_time, rows, current_user, status])
        except:
            pass
    
    def get_query_performance_stats(self) -> pd.DataFrame:
        try:
            return self.conn.execute("""
                SELECT 
                    cube_name,
                    COUNT(*) as query_count,
                    AVG(execution_time) as avg_time,
                    MAX(execution_time) as max_time,
                    AVG(rows_returned) as avg_rows
                FROM query_history
                WHERE status = 'SUCCESS'
                GROUP BY cube_name
                ORDER BY avg_time DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def create_materialized_view(self, cube_name: str, view_name: str, 
                                 dimensions: List[str], measures: List[str]):
        cube = self.cubes[cube_name]
        measures_with_agg = [(m, cube.measures[m].default_agg) for m in measures]
        df = self.query_cube(cube_name, dimensions, measures_with_agg)
        view_table = f"mv_{view_name.lower().replace(' ', '_')}"
        self.conn.register('mv_df', df)
        self.conn.execute(f"CREATE OR REPLACE TABLE {view_table} AS SELECT * FROM mv_df")
        
        log_audit("CREATE_MVIEW", {"cube": cube_name, "view": view_table})
        
        return view_table
    
    def get_cubes_list(self) -> pd.DataFrame:
        try:
            return self.conn.execute("""
                SELECT name, description, created_at, updated_at, owner, is_public
                FROM olap_cubes
                ORDER BY updated_at DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def delete_cube(self, name: str) -> bool:
        try:
            cube = self.cubes.get(name)
            if cube:
                try:
                    self.conn.execute(f"DROP TABLE IF EXISTS {cube.table_name}")
                except:
                    pass
            self.conn.execute("DELETE FROM olap_cubes WHERE name = ?", [name])
            self.conn.execute("DELETE FROM olap_slices WHERE cube_name = ?", [name])
            self.conn.execute("DELETE FROM dashboards WHERE cube_name = ?", [name])
            if name in self.cubes:
                del self.cubes[name]
            
            log_audit("DELETE_CUBE", {"cube": name})
            
            return True
        except:
            return False

# ============================================
# 9. СИСТЕМА ПОЛЬЗОВАТЕЛЕЙ
# ============================================
class UserManager:
    def __init__(self, conn):
        self.conn = conn
    
    def authenticate(self, username: str, password: str) -> bool:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            # Упрощённый запрос без is_active (на случай если колонки нет)
            try:
                result = self.conn.execute("""
                    SELECT role FROM users 
                    WHERE username = ? AND password_hash = ? AND is_active = TRUE
                """, [username, password_hash]).fetchone()
            except:
                # Запасной вариант без is_active
                result = self.conn.execute("""
                    SELECT role FROM users 
                    WHERE username = ? AND password_hash = ?
                """, [username, password_hash]).fetchone()
            
            if result:
                try:
                    self.conn.execute("UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE username = ?", [username])
                except:
                    pass
                st.session_state.username = username
                st.session_state.role = result[0]
                
                log_audit("LOGIN", {"username": username})
                
                return True
        except Exception as e:
            st.error(f"Ошибка аутентификации: {e}")
        return False
    
    def create_user(self, username: str, password: str, role: str = 'VIEWER', email: str = "") -> bool:
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
            
            log_audit("CREATE_USER", {"username": username, "role": role})
            
            return True
        except:
            return False
    
    def update_user(self, username: str, role: str = None, email: str = None, 
                    password: str = None, is_active: bool = None) -> bool:
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
            if password:
                password_hash = hashlib.sha256(password.encode()).hexdigest()
                updates.append("password_hash = ?")
                params.append(password_hash)
            if is_active is not None:
                updates.append("is_active = ?")
                params.append(is_active)
            
            if updates:
                params.append(username)
                self.conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE username = ?", params)
            
            log_audit("UPDATE_USER", {"username": username})
            
            return True
        except:
            return False
    
    def delete_user(self, username: str) -> bool:
        if st.session_state.get('role') != 'ADMIN' or username == 'admin':
            return False
        
        try:
            self.conn.execute("DELETE FROM users WHERE username = ?", [username])
            
            log_audit("DELETE_USER", {"username": username})
            
            return True
        except:
            return False
    
    def check_permission(self, cube_name: str, required_level: str = 'READ') -> bool:
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
        try:
            return self.conn.execute("""
                SELECT username, role, email, created_at, last_login, is_active
                FROM users 
                ORDER BY created_at DESC
            """).fetchdf()
        except:
            return pd.DataFrame()
    
    def grant_permission(self, role: str, cube_name: str, access_level: str) -> bool:
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
                max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO permissions (id, user_role, cube_name, access_level, granted_by)
                    VALUES (?, ?, ?, ?, ?)
                """, [max_id + 1, role, cube_name, access_level, granted_by])
            
            log_audit("GRANT_PERMISSION", {"role": role, "cube": cube_name, "level": access_level})
            
            return True
        except:
            return False
    
    def revoke_permission(self, role: str, cube_name: str) -> bool:
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        try:
            self.conn.execute("DELETE FROM permissions WHERE user_role = ? AND cube_name = ?", [role, cube_name])
            
            log_audit("REVOKE_PERMISSION", {"role": role, "cube": cube_name})
            
            return True
        except:
            return False
    
    def get_permissions_list(self) -> pd.DataFrame:
        try:
            return self.conn.execute("""
                SELECT user_role, cube_name, access_level, granted_by, granted_at
                FROM permissions
                ORDER BY user_role, cube_name
            """).fetchdf()
        except:
            return pd.DataFrame()

# ============================================
# 10. ДАШБОРДЫ И ВИЗУАЛИЗАЦИИ
# ============================================
class DashboardManager:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
        self.conn = olap_manager.conn
    
    def create_treemap(self, cube_name: str, dimension: str, measure: str, top_n: int = 20):
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
        df = self.olap_manager.query_cube(
            cube_name, [date_dim], [(measure, 'SUM')],
            order_by=[(date_dim, 'ASC')]
        )
        
        if df.empty:
            return None
        
        fig = px.line(df, x=date_dim, y=measure, title=f"Динамика {measure}")
        fig.update_layout(height=500)
        return fig
    
    def create_pie_chart(self, cube_name: str, dimension: str, measure: str, top_n: int = 10):
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            top_n=top_n, order_by=[(measure, 'DESC')]
        )
        
        if df.empty:
            return None
        
        fig = px.pie(df, values=measure, names=dimension, title=f"Доля {measure} по {dimension}")
        fig.update_layout(height=500)
        return fig
    
    def create_heatmap(self, cube_name: str, row_dim: str, col_dim: str, measure: str):
        pivot_df = self.olap_manager.slice_dice(cube_name, [row_dim], [col_dim], [measure])
        
        if pivot_df.empty:
            return None
        
        fig = px.imshow(
            pivot_df, title=f"Heatmap: {measure}",
            color_continuous_scale='RdBu_r', aspect='auto'
        )
        fig.update_layout(height=500)
        return fig
    
    def create_scatter_plot(self, cube_name: str, x_measure: str, y_measure: str, color_dim: str = None):
        dims = [color_dim] if color_dim else []
        measures = [(x_measure, 'SUM'), (y_measure, 'SUM')]
        
        df = self.olap_manager.query_cube(cube_name, dims, measures)
        
        if df.empty:
            return None
        
        if color_dim and color_dim in df.columns:
            fig = px.scatter(df, x=x_measure, y=y_measure, color=color_dim,
                           title=f"Корреляция {x_measure} и {y_measure}")
        else:
            fig = px.scatter(df, x=x_measure, y=y_measure,
                           title=f"Корреляция {x_measure} и {y_measure}")
        fig.update_layout(height=500)
        return fig
    
    def create_waterfall(self, cube_name: str, dimension: str, measure: str):
        df = self.olap_manager.query_cube(
            cube_name, [dimension], [(measure, 'SUM')],
            order_by=[(dimension, 'ASC')]
        )
        
        if df.empty:
            return None
        
        fig = go.Figure(go.Waterfall(
            name="Изменения",
            orientation="v",
            measure=["relative"] * len(df),
            x=df[dimension],
            y=df[measure],
            text=[f"{v:,.0f}" for v in df[measure]],
            textposition="outside",
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        
        fig.update_layout(title=f"Waterfall анализ {measure}", height=500)
        return fig
    
    def create_box_plot(self, cube_name: str, dimension: str, measure: str):
        df = self.olap_manager.query_cube(cube_name, [dimension], [(measure, 'SUM')])
        
        if df.empty:
            return None
        
        fig = px.box(df, x=dimension, y=measure, title=f"Box Plot: {measure} по {dimension}")
        fig.update_layout(height=500)
        return fig
    
    def create_histogram(self, cube_name: str, measure: str, bins: int = 20):
        df = self.olap_manager.query_cube(cube_name, [], [(measure, 'SUM')])
        
        if df.empty:
            return None
        
        fig = px.histogram(df, x=measure, nbins=bins, title=f"Гистограмма: {measure}")
        fig.update_layout(height=500)
        return fig
    
    def create_kpi_cards(self, cube_name: str, measures: List[str]) -> Dict:
        kpis = {}
        for measure in measures:
            df = self.olap_manager.query_cube(cube_name, [], [(measure, 'SUM')])
            current = df[measure].iloc[0] if not df.empty else 0
            kpis[measure] = {'current': current, 'change': 0, 'change_pct': 0}
        return kpis
    
    def save_dashboard(self, name: str, cube_name: str, config: Dict) -> bool:
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
            
            log_audit("SAVE_DASHBOARD", {"name": name, "cube": cube_name})
            
            return True
        except:
            return False
    
    def load_dashboards(self, cube_name: str = None) -> pd.DataFrame:
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
    
    def delete_dashboard(self, dashboard_id: int) -> bool:
        try:
            self.conn.execute("DELETE FROM dashboards WHERE id = ?", [dashboard_id])
            log_audit("DELETE_DASHBOARD", {"id": dashboard_id})
            return True
        except:
            return False
    
    def export_dashboard_to_html(self, figures: List[go.Figure]) -> str:
        html_content = """<!DOCTYPE html>
<html>
<head>
    <title>OLAP Dashboard</title>
    <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
    <style>
        body { 
            font-family: 'Segoe UI', Arial, sans-serif; 
            background: #f5f5f5;
            margin: 0;
            padding: 20px;
        }
        .dashboard-container {
            max-width: 1400px;
            margin: 0 auto;
        }
        .chart { 
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #1e3c72;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class='dashboard-container'>
        <h1>🎲 OLAP Analytics Dashboard</h1>
"""
        
        for i, fig in enumerate(figures):
            html_content += f"<div class='chart'>{fig.to_html(include_plotlyjs=False)}</div>"
        
        html_content += "</div></body></html>"
        return html_content

# ============================================
# 11. API ДЛЯ ВНЕШНИХ СИСТЕМ
# ============================================
class OLAPAPI:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
    
    def execute_mdx_query(self, cube_name: str, mdx_query: str) -> Dict:
        result = {
            'cube': cube_name,
            'query': mdx_query,
            'result': None,
            'error': None
        }
        
        try:
            if 'SELECT' in mdx_query.upper():
                measures_start = mdx_query.upper().find('SELECT') + 6
                measures_end = mdx_query.upper().find('ON COLUMNS')
                if measures_end == -1:
                    measures_end = mdx_query.upper().find('ON ROWS')
                measures_str = mdx_query[measures_start:measures_end].strip()
                measures = [m.strip(' {}[]') for m in measures_str.split(',') if m.strip()]
                
                rows_start = mdx_query.upper().find('ON ROWS FROM')
                if rows_start > 0:
                    dimensions_start = rows_start + 11
                    dimensions_str = mdx_query[dimensions_start:].strip()
                    dimensions = [d.strip(' {}[]') for d in dimensions_str.split(',') if d.strip()]
                else:
                    dimensions = []
                
                measures_with_agg = [(m, 'SUM') for m in measures if m]
                df = self.olap_manager.query_cube(cube_name, dimensions, measures_with_agg)
                
                result['result'] = df.to_dict('records')
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def execute_query(self, cube_name: str, query_config: Dict) -> Dict:
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
            df.to_csv(output, index=False)
        elif format == 'excel':
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
        elif format == 'json':
            output.write(df.to_json(orient='records', indent=2).encode())
        elif format == 'parquet':
            df.to_parquet(output)
        
        return output.getvalue()
    
    def export_to_power_bi(self, cube_name: str) -> bytes:
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
                {'Dimension': d, 'Column': cube.dimensions[d].column, 'Hierarchy': str(cube.dimensions[d].hierarchy)}
                for d in dimensions
            ] + [
                {'Measure': m, 'Column': cube.measures[m].column, 'Aggregation': cube.measures[m].default_agg}
                for m in cube.measures.keys()
            ])
            metadata.to_excel(writer, sheet_name='Metadata', index=False)
        
        return output.getvalue()
    
    def get_cube_metadata(self, cube_name: str) -> Dict:
        cube = self.olap_manager.cubes.get(cube_name)
        if not cube:
            return {'error': 'Cube not found'}
        
        return {
            'name': cube.name,
            'description': cube.description,
            'table_name': cube.table_name,
            'dimensions': [
                {'name': name, 'column': dim.column, 'hierarchy': dim.hierarchy, 'description': dim.description}
                for name, dim in cube.dimensions.items()
            ],
            'measures': [
                {'name': name, 'column': m.column, 'default_agg': m.default_agg, 'description': m.description}
                for name, m in cube.measures.items()
            ],
            'calculated_members': cube.calculated_members
        }
    
    def get_api_docs(self) -> Dict:
        return {
            'version': '2.0',
            'endpoints': {
                '/api/query': {
                    'method': 'POST',
                    'description': 'Execute OLAP query',
                    'body': {
                        'cube': 'string (required)',
                        'dimensions': 'array of strings',
                        'measures': 'array of strings',
                        'filters': 'object',
                        'aggregations': 'object (measure -> aggregation function)',
                        'top_n': 'integer',
                        'order_by': 'array of [column, direction]'
                    }
                },
                '/api/export': {
                    'method': 'POST',
                    'description': 'Export data',
                    'body': {
                        'cube': 'string (required)',
                        'format': 'csv|excel|json|parquet (default: csv)',
                        'query': 'object (same as /api/query)'
                    }
                },
                '/api/metadata/{cube}': {
                    'method': 'GET',
                    'description': 'Get cube metadata'
                },
                '/api/mdx': {
                    'method': 'POST',
                    'description': 'Execute MDX-like query',
                    'body': {
                        'cube': 'string (required)',
                        'mdx': 'string (MDX query)'
                    }
                },
                '/api/powerbi/{cube}': {
                    'method': 'GET',
                    'description': 'Export data for Power BI (Excel format with metadata)'
                },
                '/api/cubes': {
                    'method': 'GET',
                    'description': 'List all available cubes'
                }
            }
        }

# ============================================
# 12. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
class OLAPInterface:
    def __init__(self):
        self.conn = get_connection()
        self.olap_manager = OLAPManager(self.conn)
        self.user_manager = UserManager(self.conn)
        self.dashboard_manager = DashboardManager(self.olap_manager)
        self.api = OLAPAPI(self.olap_manager)
        
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
        if not st.session_state.authenticated:
            self.render_login_page()
        else:
            self.render_main_interface()
    
    def render_login_page(self):
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
        st.markdown(f"""
        <div class='main-header'>
            <h1>🎲 OLAP Analytics Platform</h1>
            <p>{st.session_state.username} <span class='user-role-badge'>{st.session_state.role}</span></p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.sidebar:
            st.markdown("## 🎯 Навигация")
            
            mode = st.radio("Режим работы", [
                "📊 Анализ", "📈 Дашборды", "🏗️ Конструктор", 
                "💾 Срезы", "⚙️ Администрирование", "🔌 API"
            ])
            
            if st.button("🚪 Выход", use_container_width=True):
                log_audit("LOGOUT", {"username": st.session_state.get('username')})
                for key in ['authenticated', 'username', 'role', 'current_cube', 'drill_path']:
                    st.session_state.pop(key, None)
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
        st.markdown("### 📦 Кубы")
        
        cubes_df = self.olap_manager.get_cubes_list()
        
        if not cubes_df.empty:
            cube_names = cubes_df['name'].tolist()
            selected = st.selectbox("Выберите куб", cube_names, key="sidebar_cube_select")
            
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
            
            if st.session_state.current_cube:
                st.info(f"📌 Активный: {st.session_state.current_cube.name}")
        else:
            st.info("Нет кубов. Создайте в Конструкторе.")
    
    def render_sidebar_stats(self):
        with st.expander("📊 Статистика системы"):
            stats = self.olap_manager.query_cache.get_stats()
            st.metric("Кэш запросов", f"{stats['size']}")
            st.metric("Hit Rate", stats['hit_rate'])
            st.metric("Память", f"{stats['memory_usage']:.2f} MB")
            
            if st.button("🗑️ Очистить кэш", use_container_width=True):
                self.olap_manager.query_cache.clear()
                st.success("Кэш очищен")
                st.rerun()
    
    def render_sidebar_filters(self):
        if st.session_state.current_cube:
            with st.expander("🔍 Фильтры"):
                cube = st.session_state.current_cube
                
                for dim_name, dim in cube.dimensions.items():
                    try:
                        values = self.conn.execute(
                            f'SELECT DISTINCT "{dim.column}" FROM {cube.table_name} LIMIT 100'
                        ).fetchdf()
                        
                        if not values.empty:
                            selected = st.multiselect(
                                dim_name,
                                values[dim.column].tolist(),
                                key=f"filter_{dim_name}"
                            )
                            if selected:
                                st.session_state.filters[dim.column] = selected
                            elif dim.column in st.session_state.filters:
                                del st.session_state.filters[dim.column]
                    except:
                        pass
                
                if st.button("🔄 Сбросить фильтры", use_container_width=True):
                    st.session_state.filters = {}
                    st.rerun()
    
    def render_analysis_mode(self):
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню")
            return
        
        cube = st.session_state.current_cube
        
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "🎯 Сводная таблица",
            "📊 Визуализации",
            "🔍 Drill-down",
            "📋 Данные",
            "⚡ Оптимизация"
        ])
        
        with tab1:
            self.render_pivot_table(cube)
        
        with tab2:
            self.render_charts(cube)
        
        with tab3:
            self.render_drill_down(cube)
        
        with tab4:
            self.render_raw_data(cube)
        
        with tab5:
            self.render_optimization(cube)
    
    def render_pivot_table(self, cube: OLAPCube):
        st.markdown("### 🎯 Интерактивная сводная таблица")
        
        if not cube.dimensions:
            st.warning("В кубе нет измерений")
            return
        
        if not cube.measures:
            st.warning("В кубе нет мер")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            row_dims = st.multiselect("Строки", list(cube.dimensions.keys()), key="pivot_rows")
        with col2:
            col_dims = st.multiselect("Колонки", list(cube.dimensions.keys()), key="pivot_cols")
        
        measures = st.multiselect("Меры", list(cube.measures.keys()), key="pivot_measures")
        
        with st.expander("⚙️ Расширенные настройки"):
            col_opt1, col_opt2, col_opt3 = st.columns(3)
            
            with col_opt1:
                top_n = st.number_input("Топ N", 0, 10000, 0)
            with col_opt2:
                export_format = st.selectbox("Экспорт", ["CSV", "Excel", "JSON", "Parquet"])
            with col_opt3:
                use_cache = st.checkbox("Использовать кэш", True)
        
        if st.button("🎯 Построить", type="primary") and measures:
            with st.spinner("Выполнение запроса..."):
                pivot_df = self.olap_manager.slice_dice(
                    cube.name, row_dims, col_dims, measures,
                    st.session_state.get('filters', {})
                )
                
                if not pivot_df.empty:
                    st.dataframe(pivot_df, use_container_width=True, height=600)
                    
                    if export_format == "CSV":
                        csv = pivot_df.to_csv()
                        st.download_button("📥 Скачать CSV", csv, f"{cube.name}_pivot.csv")
                    elif export_format == "Excel":
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            pivot_df.to_excel(writer, sheet_name='Pivot')
                        st.download_button("📥 Скачать Excel", output.getvalue(), f"{cube.name}_pivot.xlsx")
                    elif export_format == "JSON":
                        json_str = pivot_df.to_json(orient='records', indent=2)
                        st.download_button("📥 Скачать JSON", json_str, f"{cube.name}_pivot.json")
                    elif export_format == "Parquet":
                        output = io.BytesIO()
                        pivot_df.to_parquet(output)
                        st.download_button("📥 Скачать Parquet", output.getvalue(), f"{cube.name}_pivot.parquet")
                else:
                    st.info("Нет данных для отображения")
    
    def render_charts(self, cube: OLAPCube):
        st.markdown("### 📊 Визуализации")
        
        chart_type = st.selectbox(
            "Тип визуализации",
            ["Treemap", "Bar Chart", "Line Chart", "Pie Chart", "Heatmap", "Scatter", "Waterfall", "Box Plot", "Histogram"]
        )
        
        if chart_type == "Treemap":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Количество элементов", 5, 50, 20)
            
            if st.button("Создать Treemap"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_treemap(cube.name, dim, measure, top_n)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Bar Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Количество элементов", 5, 50, 10)
            
            if st.button("Создать Bar Chart"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, top_n)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Line Chart":
            date_dims = [d for d, dim in cube.dimensions.items() if dim.hierarchy]
            if date_dims:
                dim = st.selectbox("Измерение даты", date_dims)
                measure = st.selectbox("Мера", list(cube.measures.keys()))
                
                if st.button("Создать Line Chart"):
                    with st.spinner("Создание визуализации..."):
                        fig = self.dashboard_manager.create_line_chart(cube.name, dim, measure)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нет измерений с иерархией дат")
        
        elif chart_type == "Pie Chart":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Количество элементов", 3, 20, 8)
            
            if st.button("Создать Pie Chart"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_pie_chart(cube.name, dim, measure, top_n)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Heatmap":
            if len(cube.dimensions) >= 2:
                row_dim = st.selectbox("Строки", list(cube.dimensions.keys()), key="heat_row")
                col_dim = st.selectbox("Колонки", list(cube.dimensions.keys()), key="heat_col")
                measure = st.selectbox("Мера", list(cube.measures.keys()))
                
                if st.button("Создать Heatmap"):
                    with st.spinner("Создание визуализации..."):
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
                
                if st.button("Создать Scatter Plot"):
                    with st.spinner("Создание визуализации..."):
                        fig = self.dashboard_manager.create_scatter_plot(
                            cube.name, x_measure, y_measure,
                            color_dim if color_dim != "Нет" else None
                        )
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Нужно минимум 2 меры")
        
        elif chart_type == "Waterfall":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            
            if st.button("Создать Waterfall"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_waterfall(cube.name, dim, measure)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Box Plot":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            
            if st.button("Создать Box Plot"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_box_plot(cube.name, dim, measure)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
        
        elif chart_type == "Histogram":
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            bins = st.slider("Количество столбцов", 5, 100, 20)
            
            if st.button("Создать Histogram"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_histogram(cube.name, measure, bins)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
    
    def render_drill_down(self, cube: OLAPCube):
        st.markdown("### 🔍 Drill-down по иерархиям")
        
        hierarchical_dims = {name: dim for name, dim in cube.dimensions.items() if dim.hierarchy}
        
        if hierarchical_dims:
            dim_name = st.selectbox("Измерение", list(hierarchical_dims.keys()))
            dim = hierarchical_dims[dim_name]
            
            st.markdown(f"**Иерархия:** {' → '.join(dim.hierarchy)}")
            
            current_level = len(st.session_state.drill_path)
            
            if current_level < len(dim.hierarchy):
                next_level = dim.hierarchy[current_level]
                
                if st.button(f"⬇️ Drill down: {next_level}"):
                    query = f"""
                        SELECT DATE_PART('{next_level.lower()}', "{dim.column}") as {next_level},
                               COUNT(*) as count
                        FROM {cube.table_name}
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
                            selected_value = st.selectbox(f"Выберите {next_level}", df[next_level].tolist())
                            if st.button("Продолжить drill-down"):
                                st.session_state.drill_path.append(selected_value)
                                st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка drill-down: {e}")
            
            if st.session_state.drill_path:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("⬆️ Drill up"):
                        st.session_state.drill_path.pop()
                        st.rerun()
                with col2:
                    if st.button("🔄 Сбросить"):
                        st.session_state.drill_path = []
                        st.rerun()
                
                st.markdown(f"**Текущий путь:** {' → '.join(st.session_state.drill_path)}")
        else:
            st.info("Нет измерений с иерархиями")
    
    def render_raw_data(self, cube: OLAPCube):
        st.markdown("### 📋 Данные куба")
        
        try:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {cube.table_name}").fetchone()[0]
            st.metric("Всего строк", f"{count:,}".replace(",", " "))
            
            limit = st.slider("Количество строк для отображения", 10, 10000, 1000)
            
            df = self.conn.execute(f"SELECT * FROM {cube.table_name} LIMIT {limit}").fetchdf()
            st.dataframe(df, use_container_width=True, height=500)
            
            if st.button("📥 Экспортировать все данные"):
                all_df = self.conn.execute(f"SELECT * FROM {cube.table_name}").fetchdf()
                csv = all_df.to_csv(index=False)
                st.download_button("Скачать CSV", csv, f"{cube.name}_data.csv")
        except Exception as e:
            st.error(f"Ошибка: {e}")
    
    def render_optimization(self, cube: OLAPCube):
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
            view_name = st.text_input("Название", f"MV_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            if st.button("Создать материализованное представление"):
                with st.spinner("Создание..."):
                    table_name = self.olap_manager.create_materialized_view(
                        cube.name, view_name, mv_dims, mv_measures
                    )
                    st.success(f"✅ Создано: {table_name}")
        
        if st.button("🗑️ Очистить кэш запросов"):
            self.olap_manager.query_cache.clear()
            st.success("Кэш очищен")
    
    def render_dashboard_mode(self):
        st.markdown("### 📈 Интерактивные дашборды")
        
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню")
            return
        
        cube = st.session_state.current_cube
        
        tab1, tab2 = st.tabs(["📊 Текущий дашборд", "💾 Управление дашбордами"])
        
        with tab1:
            st.markdown("#### 🎯 Ключевые показатели")
            measures = list(cube.measures.keys())[:4]
            
            if measures:
                cols = st.columns(len(measures))
                kpis = self.dashboard_manager.create_kpi_cards(cube.name, measures)
                
                for i, (measure, values) in enumerate(kpis.items()):
                    with cols[i]:
                        st.markdown(f"""
                        <div class='kpi-card'>
                            <div class='kpi-value'>{values['current']:,.0f}</div>
                            <div class='kpi-label'>{measure}</div>
                        </div>
                        """, unsafe_allow_html=True)
            
            st.markdown("#### 📊 Визуализации")
            
            chart_cols = st.columns(2)
            figures = []
            
            with chart_cols[0]:
                if len(cube.dimensions) > 0 and len(cube.measures) > 0:
                    dim = list(cube.dimensions.keys())[0]
                    measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_bar_chart(cube.name, dim, measure, 10)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                        figures.append(fig)
            
            with chart_cols[1]:
                if len(cube.dimensions) > 1 and len(cube.measures) > 0:
                    dim1 = list(cube.dimensions.keys())[0]
                    dim2 = list(cube.dimensions.keys())[1]
                    measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_heatmap(cube.name, dim1, dim2, measure)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                        figures.append(fig)
            
            if figures:
                if st.button("📥 Экспортировать дашборд в HTML"):
                    html_content = self.dashboard_manager.export_dashboard_to_html(figures)
                    st.download_button(
                        "Скачать HTML",
                        html_content,
                        f"{cube.name}_dashboard.html",
                        "text/html"
                    )
        
        with tab2:
            st.markdown("#### 💾 Сохранить дашборд")
            dash_name = st.text_input("Название дашборда")
            dash_config = {
                'cube': cube.name,
                'measures': st.session_state.get('pivot_measures', []),
                'dimensions': st.session_state.get('pivot_rows', []) + st.session_state.get('pivot_cols', []),
                'filters': st.session_state.get('filters', {})
            }
            
            if st.button("Сохранить дашборд") and dash_name:
                if self.dashboard_manager.save_dashboard(dash_name, cube.name, dash_config):
                    st.success("✅ Дашборд сохранен")
                else:
                    st.error("❌ Ошибка сохранения")
            
            st.markdown("#### 📂 Сохраненные дашборды")
            dashboards = self.dashboard_manager.load_dashboards(cube.name)
            
            if not dashboards.empty:
                for _, row in dashboards.iterrows():
                    with st.expander(f"{row['name']} - {row['created_at']}"):
                        st.markdown(f"**Владелец:** {row['owner']}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("📂 Загрузить", key=f"load_dash_{row['id']}"):
                                st.success("✅ Дашборд загружен")
                        with col2:
                            if st.button("🗑️ Удалить", key=f"del_dash_{row['id']}"):
                                if self.dashboard_manager.delete_dashboard(row['id']):
                                    st.success("✅ Дашборд удален")
                                    st.rerun()
            else:
                st.info("Нет сохраненных дашбордов")
    
    def render_cube_designer(self):
        st.markdown("### 🏗️ Конструктор OLAP кубов")
        
        if not self.user_manager.check_permission('*', 'WRITE'):
            st.error("❌ Недостаточно прав для создания кубов")
            return
        
        tab1, tab2 = st.tabs(["📤 Создать куб", "📋 Управление кубами"])
        
        with tab1:
            uploaded_files = st.file_uploader(
                "Загрузите данные",
                type=['csv', 'xlsx', 'xls', 'parquet'],
                accept_multiple_files=True
            )
            
            if uploaded_files:
                cube_name = st.text_input("Название куба", f"Cube_{datetime.now().strftime('%Y%m%d_%H%M')}")
                cube_desc = st.text_area("Описание", "")
                
                dfs = []
                for file in uploaded_files:
                    try:
                        if file.name.endswith('.csv'):
                            df = pd.read_csv(file)
                        elif file.name.endswith('.parquet'):
                            df = pd.read_parquet(file)
                        else:
                            df = pd.read_excel(file)
                        dfs.append(df)
                    except Exception as e:
                        st.error(f"Ошибка загрузки {file.name}: {e}")
                
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    
                    st.markdown("**Предпросмотр:**")
                    st.dataframe(combined_df.head(10), use_container_width=True)
                    
                    st.markdown(f"**Размер данных:** {len(combined_df):,} строк, {len(combined_df.columns)} колонок".replace(",", " "))
                    
                    partition_col = st.selectbox(
                        "Партиционировать по (опционально)",
                        ['Нет'] + list(combined_df.columns)
                    )
                    
                    with st.expander("🔧 Ручная настройка измерений и мер"):
                        st.markdown("**Измерения:**")
                        dimensions = st.multiselect(
                            "Выберите измерения",
                            combined_df.columns,
                            default=[c for c in combined_df.columns if combined_df[c].dtype == 'object'][:5]
                        )
                        
                        st.markdown("**Меры:**")
                        measures = st.multiselect(
                            "Выберите меры",
                            combined_df.columns,
                            default=[c for c in combined_df.columns if pd.api.types.is_numeric_dtype(combined_df[c])]
                        )
                    
                    if st.button("🎲 Создать куб", type="primary"):
                        with st.spinner("Создание куба и оптимизация..."):
                            cube = self.olap_manager.create_cube_from_dataframe(
                                cube_name,
                                combined_df,
                                cube_desc,
                                auto_detect=True,
                                partition_by=partition_col if partition_col != 'Нет' else None
                            )
                            
                            if cube:
                                st.session_state.current_cube = cube
                                st.success(f"✅ Куб '{cube_name}' создан!")
                                
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Строк", f"{len(combined_df):,}".replace(",", " "))
                                with col2:
                                    st.metric("Колонок", len(combined_df.columns))
                                with col3:
                                    st.metric("Измерений", len(cube.dimensions))
                                with col4:
                                    st.metric("Мер", len(cube.measures))
        
        with tab2:
            cubes_df = self.olap_manager.get_cubes_list()
            
            if not cubes_df.empty:
                st.dataframe(cubes_df, use_container_width=True)
                
                st.markdown("---")
                st.markdown("### 🗑️ Удаление куба")
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    to_delete = st.selectbox("Выберите куб для удаления", cubes_df['name'].tolist())
                with col2:
                    if st.button("🗑️ Удалить", type="secondary", use_container_width=True):
                        if self.user_manager.check_permission(to_delete, 'WRITE'):
                            if self.olap_manager.delete_cube(to_delete):
                                st.success(f"✅ Куб '{to_delete}' удалён")
                                if st.session_state.current_cube and st.session_state.current_cube.name == to_delete:
                                    st.session_state.current_cube = None
                                st.rerun()
                            else:
                                st.error("❌ Ошибка удаления")
                        else:
                            st.error("❌ Недостаточно прав")
            else:
                st.info("Нет созданных кубов")
    
    def render_slice_manager(self):
        st.markdown("### 💾 Управление срезами данных")
        
        if not st.session_state.current_cube:
            st.info("👈 Выберите куб в боковом меню")
            return
        
        cube = st.session_state.current_cube
        
        tab1, tab2 = st.tabs(["💾 Сохранить срез", "📂 Загрузить срез"])
        
        with tab1:
            st.markdown("#### 💾 Сохранить текущий срез")
            slice_name = st.text_input("Название среза")
            slice_desc = st.text_area("Описание")
            
            current_config = {
                'cube': cube.name,
                'filters': st.session_state.get('filters', {}),
                'rows': st.session_state.get('pivot_rows', []),
                'cols': st.session_state.get('pivot_cols', []),
                'measures': st.session_state.get('pivot_measures', []),
                'drill_path': st.session_state.get('drill_path', [])
            }
            
            st.json(current_config)
            
            if st.button("💾 Сохранить срез") and slice_name:
                try:
                    max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_slices").fetchone()[0]
                    self.conn.execute("""
                        INSERT INTO olap_slices (id, cube_name, slice_name, definition, description, owner)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [max_id + 1, cube.name, slice_name, json.dumps(current_config), slice_desc, st.session_state.username])
                    st.success("✅ Срез сохранен!")
                    log_audit("SAVE_SLICE", {"cube": cube.name, "slice": slice_name})
                except Exception as e:
                    st.error(f"Ошибка сохранения: {e}")
        
        with tab2:
            st.markdown("#### 📂 Сохраненные срезы")
            
            try:
                slices_df = self.conn.execute("""
                    SELECT id, slice_name, definition, description, created_at, owner
                    FROM olap_slices 
                    WHERE cube_name = ?
                    ORDER BY created_at DESC
                """, [cube.name]).fetchdf()
                
                if not slices_df.empty:
                    for _, row in slices_df.iterrows():
                        with st.expander(f"{row['slice_name']} - {row['created_at']}"):
                            slice_def = json.loads(row['definition'])
                            st.markdown(f"**Владелец:** {row['owner']}")
                            st.markdown(f"**Описание:** {row['description'] or 'Нет'}")
                            
                            st.json(slice_def)
                            
                            col1, col2, col3 = st.columns(3)
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
                else:
                    st.info("Нет сохраненных срезов")
            except Exception as e:
                st.error(f"Ошибка загрузки срезов: {e}")
    
    def render_admin_panel(self):
        st.markdown("### ⚙️ Администрирование системы")
        
        if st.session_state.get('role') != 'ADMIN':
            st.error("❌ Доступ только для администраторов")
            return
        
        admin_tabs = st.tabs([
            "👥 Пользователи",
            "🔐 Права доступа",
            "📊 Мониторинг",
            "🗄️ База данных",
            "📝 Аудит"
        ])
        
        with admin_tabs[0]:
            st.markdown("#### 👥 Управление пользователями")
            
            with st.expander("➕ Создать пользователя"):
                col1, col2 = st.columns(2)
                with col1:
                    new_username = st.text_input("Логин")
                    new_password = st.text_input("Пароль", type="password")
                with col2:
                    new_role = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"])
                    new_email = st.text_input("Email")
                
                if st.button("Создать пользователя"):
                    if self.user_manager.create_user(new_username, new_password, new_role, new_email):
                        st.success("✅ Пользователь создан")
                        st.rerun()
                    else:
                        st.error("❌ Ошибка создания")
            
            users_df = self.user_manager.get_users_list()
            if not users_df.empty:
                st.dataframe(users_df, use_container_width=True)
                
                st.markdown("---")
                st.markdown("#### ✏️ Редактировать пользователя")
                
                selected_user = st.selectbox("Выберите пользователя", users_df['username'].tolist())
                user_data = users_df[users_df['username'] == selected_user].iloc[0]
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    new_role_edit = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"], 
                                                index=["VIEWER", "ANALYST", "ADMIN"].index(user_data['role']))
                with col2:
                    new_email_edit = st.text_input("Email", value=user_data.get('email', ''))
                with col3:
                    is_active = st.checkbox("Активен", value=user_data.get('is_active', True))
                
                new_password_edit = st.text_input("Новый пароль (оставьте пустым, чтобы не менять)", type="password")
                
                if st.button("Обновить пользователя"):
                    if self.user_manager.update_user(
                        selected_user, 
                        role=new_role_edit, 
                        email=new_email_edit,
                        password=new_password_edit if new_password_edit else None,
                        is_active=is_active
                    ):
                        st.success("✅ Пользователь обновлен")
                        st.rerun()
                    else:
                        st.error("❌ Ошибка обновления")
                
                if selected_user != 'admin':
                    if st.button("🗑️ Удалить пользователя", type="secondary"):
                        if self.user_manager.delete_user(selected_user):
                            st.success("✅ Пользователь удален")
                            st.rerun()
                        else:
                            st.error("❌ Ошибка удаления")
        
        with admin_tabs[1]:
            st.markdown("#### 🔐 Управление правами доступа")
            
            cubes = self.olap_manager.get_cubes_list()
            
            if not cubes.empty:
                st.markdown("##### Назначить права")
                col1, col2, col3 = st.columns(3)
                with col1:
                    role = st.selectbox("Роль", ["VIEWER", "ANALYST"])
                with col2:
                    cube = st.selectbox("Куб", ['*'] + cubes['name'].tolist())
                with col3:
                    access = st.selectbox("Уровень доступа", ["READ", "WRITE"])
                
                if st.button("Назначить права"):
                    if self.user_manager.grant_permission(role, cube, access):
                        st.success("✅ Права назначены")
                        st.rerun()
                    else:
                        st.error("❌ Ошибка назначения прав")
                
                st.markdown("---")
                st.markdown("##### Текущие права")
                
                perms_df = self.user_manager.get_permissions_list()
                if not perms_df.empty:
                    st.dataframe(perms_df, use_container_width=True)
                    
                    st.markdown("##### Отозвать права")
                    col1, col2 = st.columns(2)
                    with col1:
                        revoke_role = st.selectbox("Роль", perms_df['user_role'].unique(), key="revoke_role")
                    with col2:
                        revoke_cube = st.selectbox("Куб", 
                                                   perms_df[perms_df['user_role'] == revoke_role]['cube_name'].tolist(),
                                                   key="revoke_cube")
                    
                    if st.button("Отозвать права"):
                        if self.user_manager.revoke_permission(revoke_role, revoke_cube):
                            st.success("✅ Права отозваны")
                            st.rerun()
                        else:
                            st.error("❌ Ошибка отзыва прав")
            else:
                st.info("Нет созданных кубов")
        
        with admin_tabs[2]:
            st.markdown("#### 📊 Мониторинг системы")
            
            st.markdown("**Статистика запросов:**")
            stats_df = self.olap_manager.get_query_performance_stats()
            if not stats_df.empty:
                st.dataframe(stats_df, use_container_width=True)
            else:
                st.info("Нет данных о запросах")
            
            st.markdown("**Состояние кэша:**")
            cache_stats = self.olap_manager.query_cache.get_stats()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Записей в кэше", cache_stats['size'])
            with col2:
                st.metric("Hit Rate", cache_stats['hit_rate'])
            with col3:
                st.metric("Попаданий", cache_stats['hits'])
            with col4:
                st.metric("Промахов", cache_stats['misses'])
            
            st.markdown("**Последние запросы:**")
            try:
                recent = self.conn.execute("""
                    SELECT timestamp, user_name, cube_name, 
                           ROUND(execution_time, 3) as exec_time,
                           rows_returned, status
                    FROM query_history 
                    ORDER BY timestamp DESC 
                    LIMIT 20
                """).fetchdf()
                
                if not recent.empty:
                    st.dataframe(recent, use_container_width=True)
            except:
                pass
        
        with admin_tabs[3]:
            st.markdown("#### 🗄️ Управление базой данных")
            
            if st.button("📊 Показать все таблицы"):
                try:
                    tables = self.conn.execute("SHOW TABLES").fetchdf()
                    st.dataframe(tables, use_container_width=True)
                except:
                    st.info("Не удалось получить список таблиц")
            
            if st.button("🗜️ Оптимизировать базу (VACUUM)"):
                try:
                    self.conn.execute("VACUUM")
                    st.success("✅ База данных оптимизирована")
                except:
                    st.error("❌ Ошибка оптимизации")
            
            if os.path.exists(DB_PATH):
                size = os.path.getsize(DB_PATH) / 1024 / 1024
                st.metric("Размер файла БД", f"{size:.2f} MB")
            
            st.markdown("---")
            st.markdown("#### ⚠️ Опасная зона")
            
            if st.button("🗑️ Очистить историю запросов", type="secondary"):
                try:
                    self.conn.execute("DELETE FROM query_history")
                    st.success("✅ История запросов очищена")
                except:
                    st.error("❌ Ошибка очистки")
        
        with admin_tabs[4]:
            st.markdown("#### 📝 Журнал аудита")
            
            try:
                audit_df = self.conn.execute("""
                    SELECT timestamp, user_name, action, details
                    FROM audit_log
                    ORDER BY timestamp DESC
                    LIMIT 100
                """).fetchdf()
                
                if not audit_df.empty:
                    st.dataframe(audit_df, use_container_width=True)
                else:
                    st.info("Журнал аудита пуст")
            except:
                st.info("Таблица аудита не найдена")
    
    def render_api_documentation(self):
        st.markdown("### 🔌 API для внешних систем")
        
        docs = self.api.get_api_docs()
        
        st.markdown(f"**Версия API:** {docs['version']}")
        st.markdown("---")
        
        for endpoint, info in docs['endpoints'].items():
            with st.expander(f"{info['method']} {endpoint}"):
                st.markdown(f"**Описание:** {info['description']}")
                st.markdown("**Параметры запроса:**")
                st.json(info.get('body', {}))
        
        st.markdown("---")
        st.markdown("#### 🧪 Тестирование API")
        
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            
            st.markdown("**Метаданные текущего куба:**")
            meta = self.api.get_cube_metadata(cube.name)
            st.json(meta)
            
            st.markdown("---")
            st.markdown("**MDX запрос:**")
            
            if cube.measures and cube.dimensions:
                mdx_query = st.text_area(
                    "MDX Запрос",
                    value=f"""SELECT 
  {{[Measures].[{list(cube.measures.keys())[0]}]}} ON COLUMNS,
  {{[Dimension].[{list(cube.dimensions.keys())[0]}]}} ON ROWS
FROM [{cube.name}]""",
                    height=150
                )
                
                if st.button("Выполнить MDX запрос"):
                    result = self.api.execute_mdx_query(cube.name, mdx_query)
                    
                    if result.get('error'):
                        st.error(f"Ошибка: {result['error']}")
                    else:
                        st.success(f"Получено {len(result.get('result', []))} записей")
                        st.json(result)
            
            st.markdown("---")
            st.markdown("**Экспорт данных:**")
            
            col1, col2 = st.columns(2)
            with col1:
                export_format = st.selectbox("Формат", ["csv", "excel", "json", "parquet"])
            with col2:
                if st.button("📥 Экспортировать данные"):
                    data = self.api.export_data(cube.name, export_format)
                    if data:
                        st.download_button(
                            "Скачать файл",
                            data,
                            f"{cube.name}_export.{export_format}",
                            "application/octet-stream"
                        )
            
            st.markdown("---")
            st.markdown("**Экспорт для Power BI:**")
            
            if st.button("📊 Экспортировать для Power BI"):
                data = self.api.export_to_power_bi(cube.name)
                if data:
                    st.download_button(
                        "Скачать Power BI файл",
                        data,
                        f"{cube.name}_powerbi.xlsx",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        else:
            st.info("👈 Загрузите куб для тестирования API")

# ============================================
# 13. ЗАПУСК ПРИЛОЖЕНИЯ
# ============================================
def main():
    interface = OLAPInterface()
    interface.run()

if __name__ == "__main__":
    main()
