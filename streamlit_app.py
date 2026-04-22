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
# 2. ПРИНУДИТЕЛЬНОЕ СОЗДАНИЕ АДМИНА (ИСПРАВЛЕНО)
# ============================================
if 'admin_created' not in st.session_state:
    if os.path.exists('olap_analytics.db'):
        try:
            os.remove('olap_analytics.db')
        except:
            pass
    st.session_state.admin_created = True

@st.cache_resource
def get_connection():
    conn = duckdb.connect('olap_analytics.db')
    conn.execute("INSTALL json; LOAD json;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    return conn

conn = get_connection()

def force_create_admin():
    try:
        # Создаём таблицу пользователей БЕЗ UNIQUE на id
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username VARCHAR NOT NULL,
                password_hash VARCHAR NOT NULL,
                role VARCHAR NOT NULL DEFAULT 'VIEWER',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        """)
        
        # Создаём уникальный индекс отдельно
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        except:
            pass
        
        # Создаём таблицу прав
        conn.execute("""
            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY,
                user_role VARCHAR NOT NULL,
                cube_name VARCHAR NOT NULL,
                access_level VARCHAR NOT NULL DEFAULT 'READ'
            )
        """)
        
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_permissions_role_cube ON permissions(user_role, cube_name)")
        except:
            pass
        
        # Проверяем существование админа
        admin_exists = conn.execute("SELECT COUNT(*) FROM users WHERE username = 'admin'").fetchone()[0]
        
        if admin_exists == 0:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
            admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
            conn.execute("""
                INSERT INTO users (id, username, password_hash, role, created_at)
                VALUES (?, 'admin', ?, 'ADMIN', CURRENT_TIMESTAMP)
            """, [max_id + 1, admin_hash])
        else:
            admin_hash = hashlib.sha256("admin123".encode()).hexdigest()
            conn.execute("UPDATE users SET password_hash = ?, role = 'ADMIN' WHERE username = 'admin'", [admin_hash])
        
        # Даём админу все права (проверяем существование)
        perm_exists = conn.execute(
            "SELECT COUNT(*) FROM permissions WHERE user_role = 'ADMIN' AND cube_name = '*'"
        ).fetchone()[0]
        
        if perm_exists == 0:
            max_perm_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
            conn.execute("""
                INSERT INTO permissions (id, user_role, cube_name, access_level)
                VALUES (?, 'ADMIN', '*', 'ADMIN')
            """, [max_perm_id + 1])
        
        return True
    except Exception as e:
        st.error(f"Ошибка создания админа: {e}")
        return False

force_create_admin()

# Профессиональные CSS стили
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
        background: #1e3c72;
        color: white;
        padding: 10px;
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
</style>
""", unsafe_allow_html=True)

# ============================================
# 3. КЭШИРОВАНИЕ ЗАПРОСОВ
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
# 4. МОДЕЛЬ ДАННЫХ OLAP
# ============================================
class OLAPDimension:
    def __init__(self, name: str, column: str, hierarchy: List[str] = None):
        self.name = name
        self.column = column
        self.hierarchy = hierarchy or []
        self.attributes = {}
        
    def add_attribute(self, name: str, column: str):
        self.attributes[name] = column
        
class OLAPMeasure:
    def __init__(self, name: str, column: str, default_agg: str = 'SUM'):
        self.name = name
        self.column = column
        self.default_agg = default_agg
        self.allowed_aggs = ['SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT', 'MEDIAN', 'STDDEV']
        
class OLAPCube:
    def __init__(self, name: str, table_name: str):
        self.name = name
        self.table_name = table_name
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

# ============================================
# 5. ОПТИМИЗИРОВАННЫЙ OLAP МЕНЕДЖЕР
# ============================================
class OLAPManager:
    def __init__(self, conn):
        self.conn = conn
        self.cubes: Dict[str, OLAPCube] = {}
        self.query_cache = QueryCache()
        self._init_metadata_tables()
        self._init_partitions()
        
    def _init_metadata_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS olap_cubes (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                table_name VARCHAR,
                definition JSON,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                owner VARCHAR
            )
        """)
        try:
            self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cubes_name ON olap_cubes(name)")
        except:
            pass
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS olap_slices (
                id INTEGER PRIMARY KEY,
                cube_name VARCHAR,
                slice_name VARCHAR,
                definition JSON,
                created_at TIMESTAMP,
                owner VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY,
                cube_name VARCHAR,
                query_text VARCHAR,
                execution_time FLOAT,
                rows_returned INTEGER,
                timestamp TIMESTAMP,
                user_name VARCHAR
            )
        """)
    
    def _init_partitions(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS table_partitions (
                id INTEGER PRIMARY KEY,
                table_name VARCHAR,
                partition_column VARCHAR,
                partition_value VARCHAR,
                row_count INTEGER,
                created_at TIMESTAMP
            )
        """)
    
    def create_cube_from_dataframe(self, name: str, df: pd.DataFrame, 
                                  auto_detect: bool = True,
                                  partition_by: str = None) -> OLAPCube:
        table_name = f"cube_{name.lower().replace(' ', '_')}"
        
        if partition_by and partition_by in df.columns:
            for value in df[partition_by].unique():
                partition_df = df[df[partition_by] == value]
                partition_name = f"{table_name}_{abs(hash(str(value)))}"
                self.conn.register('temp_partition', partition_df)
                self.conn.execute(f"CREATE TABLE IF NOT EXISTS {partition_name} AS SELECT * FROM temp_partition")
                
                max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM table_partitions").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO table_partitions (id, table_name, partition_column, partition_value, row_count, created_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
        
        cube = OLAPCube(name, table_name)
        
        if auto_detect:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    measure = OLAPMeasure(col, col, 'SUM')
                    cube.add_measure(measure)
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    dim = OLAPDimension(col, col, ['Year', 'Quarter', 'Month', 'Day'])
                    cube.add_dimension(dim)
                else:
                    unique_count = df[col].nunique()
                    if unique_count < 100:
                        dim = OLAPDimension(col, col)
                        cube.add_dimension(dim)
        
        self.cubes[name] = cube
        self._save_cube_metadata(cube)
        
        return cube
    
    def _save_cube_metadata(self, cube: OLAPCube):
        definition = {
            'dimensions': {name: {'column': d.column, 'hierarchy': d.hierarchy} 
                          for name, d in cube.dimensions.items()},
            'measures': {name: {'column': m.column, 'default_agg': m.default_agg} 
                        for name, m in cube.measures.items()},
            'calculated_members': cube.calculated_members
        }
        
        current_user = st.session_state.get('username', 'admin')
        
        # Проверяем существование
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
                INSERT INTO olap_cubes (id, name, table_name, definition, created_at, updated_at, owner)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
            """, [max_id + 1, cube.name, cube.table_name, json.dumps(definition), current_user])
    
    def slice_dice(self, cube_name: str, rows: List[str], cols: List[str], 
                   measures: List[str], filters: Dict = None) -> pd.DataFrame:
        dimensions = list(set(rows + cols))
        measures_with_agg = [(m, self.cubes[cube_name].measures[m].default_agg) for m in measures]
        
        df = self.query_cube(cube_name, dimensions, measures_with_agg, filters)
        
        if df.empty:
            return df
        
        if rows and cols:
            pivot_df = df.pivot_table(
                index=rows,
                columns=cols,
                values=measures,
                aggfunc='sum',
                fill_value=0
            )
            return pivot_df
        elif rows:
            return df.groupby(rows)[measures].sum().reset_index()
        elif cols:
            return df.groupby(cols)[measures].sum().reset_index()
        else:
            return df[measures].sum().to_frame().T
    
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
        
        cube = self.cubes.get(cube_name)
        if not cube:
            raise ValueError(f"Куб {cube_name} не найден")
        
        table_name = cube.table_name
        
        select_parts = []
        group_by_parts = []
        
        for dim_name in dimensions:
            dim = cube.dimensions[dim_name]
            select_parts.append(f'"{dim.column}" as "{dim_name}"')
            group_by_parts.append(f'"{dim.column}"')
        
        for measure_name, agg_func in measures:
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
        
        query = f"SELECT {', '.join(select_parts)} FROM {table_name}"
        
        if filters:
            where_conditions = []
            for col, value in filters.items():
                if isinstance(value, list):
                    values_str = ', '.join([f"'{v}'" for v in value])
                    where_conditions.append(f'"{col}" IN ({values_str})')
                elif isinstance(value, dict):
                    if 'min' in value:
                        where_conditions.append(f'"{col}" >= {value["min"]}')
                    if 'max' in value:
                        where_conditions.append(f'"{col}" <= {value["max"]}')
                else:
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
        except Exception as e:
            st.error(f"Ошибка выполнения запроса: {e}")
            result = pd.DataFrame()
        
        execution_time = (datetime.now() - start_time).total_seconds()
        self._log_query(cube_name, query, execution_time, len(result))
        
        if use_cache and not result.empty:
            self.query_cache.set(cache_key, result)
        
        return result
    
    def _log_query(self, cube_name: str, query: str, execution_time: float, rows: int):
        current_user = st.session_state.get('username', 'anonymous')
        try:
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM query_history").fetchone()[0]
            self.conn.execute("""
                INSERT INTO query_history (id, cube_name, query_text, execution_time, rows_returned, timestamp, user_name)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            """, [max_id + 1, cube_name, query[:1000], execution_time, rows, current_user])
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
        return view_table

# ============================================
# 6. СИСТЕМА ПОЛЬЗОВАТЕЛЕЙ И ПРАВ
# ============================================
class UserManager:
    def __init__(self, conn):
        self.conn = conn
    
    def authenticate(self, username: str, password: str) -> bool:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            result = self.conn.execute("""
                SELECT role FROM users 
                WHERE username = ? AND password_hash = ?
            """, [username, password_hash]).fetchone()
            
            if result:
                self.conn.execute("""
                    UPDATE users SET last_login = CURRENT_TIMESTAMP 
                    WHERE username = ?
                """, [username])
                st.session_state.username = username
                st.session_state.role = result[0]
                return True
        except:
            pass
        return False
    
    def create_user(self, username: str, password: str, role: str = 'VIEWER') -> bool:
        if st.session_state.get('role') != 'ADMIN':
            return False
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        try:
            exists = self.conn.execute("SELECT COUNT(*) FROM users WHERE username = ?", [username]).fetchone()[0]
            if exists > 0:
                return False
            
            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM users").fetchone()[0]
            self.conn.execute("""
                INSERT INTO users (id, username, password_hash, role, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [max_id + 1, username, password_hash, role])
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
            
            access_level = result[0]
            levels = {'READ': 1, 'WRITE': 2, 'ADMIN': 3}
            return levels.get(access_level, 0) >= levels.get(required_level, 1)
        except:
            return False
    
    def get_users_list(self) -> pd.DataFrame:
        try:
            return self.conn.execute("""
                SELECT username, role, created_at, last_login 
                FROM users 
                ORDER BY created_at DESC
            """).fetchdf()
        except:
            return pd.DataFrame()

# ============================================
# 7. ВИЗУАЛИЗАЦИИ И ДАШБОРДЫ
# ============================================
class DashboardManager:
    def __init__(self, olap_manager: OLAPManager):
        self.olap_manager = olap_manager
    
    def create_treemap(self, cube_name: str, dimension: str, measure: str, top_n: int = 20):
        df = self.olap_manager.query_cube(
            cube_name,
            [dimension],
            [(measure, 'SUM')],
            top_n=top_n,
            order_by=[(measure, 'DESC')]
        )
        fig = px.treemap(
            df,
            path=[dimension],
            values=measure,
            title=f"Распределение {measure} по {dimension}"
        )
        return fig
    
    def create_waterfall(self, cube_name: str, dimension: str, measure: str):
        df = self.olap_manager.query_cube(
            cube_name,
            [dimension],
            [(measure, 'SUM')],
            order_by=[(dimension, 'ASC')]
        )
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
        fig.update_layout(title=f"Waterfall анализ {measure}")
        return fig
    
    def create_heatmap(self, cube_name: str, row_dim: str, col_dim: str, measure: str):
        pivot_df = self.olap_manager.slice_dice(cube_name, [row_dim], [col_dim], [measure])
        fig = px.imshow(
            pivot_df,
            title=f"Heatmap: {measure}",
            color_continuous_scale='RdBu_r',
            aspect='auto'
        )
        return fig
    
    def create_kpi_cards(self, cube_name: str, measures: List[str]) -> Dict:
        kpis = {}
        for measure in measures:
            df = self.olap_manager.query_cube(cube_name, [], [(measure, 'SUM')])
            current = df[measure].iloc[0] if not df.empty else 0
            kpis[measure] = {
                'current': current,
                'change': 0,
                'change_pct': 0
            }
        return kpis
    
    def export_dashboard_to_html(self, figures: List[go.Figure]) -> str:
        html_content = "<html><head><title>OLAP Dashboard</title>"
        html_content += "<script src='https://cdn.plot.ly/plotly-latest.min.js'></script>"
        html_content += "</head><body>"
        for fig in figures:
            html_content += fig.to_html(include_plotlyjs=False)
        html_content += "</body></html>"
        return html_content

# ============================================
# 8. API ДЛЯ ВНЕШНИХ СИСТЕМ
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
                measures_str = mdx_query[measures_start:measures_end].strip()
                measures = [m.strip(' {}') for m in measures_str.split(',')]
                
                rows_start = mdx_query.upper().find('ON ROWS FROM')
                if rows_start > 0:
                    dimensions_start = rows_start + 11
                    dimensions_str = mdx_query[dimensions_start:].strip()
                    dimensions = [d.strip(' {}') for d in dimensions_str.split(',')]
                else:
                    dimensions = []
                
                measures_with_agg = [(m, 'SUM') for m in measures]
                df = self.olap_manager.query_cube(cube_name, dimensions, measures_with_agg)
                result['result'] = df.to_dict('records')
        except Exception as e:
            result['error'] = str(e)
        return result
    
    def export_to_power_bi(self, cube_name: str) -> bytes:
        cube = self.olap_manager.cubes[cube_name]
        dimensions = list(cube.dimensions.keys())
        measures = [(m, 'SUM') for m in cube.measures.keys()]
        df = self.olap_manager.query_cube(cube_name, dimensions, measures)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            metadata = pd.DataFrame([
                {'Dimension': d, 'Column': cube.dimensions[d].column}
                for d in dimensions
            ] + [
                {'Measure': m, 'Column': cube.measures[m].column, 'Aggregation': cube.measures[m].default_agg}
                for m in cube.measures.keys()
            ])
            metadata.to_excel(writer, sheet_name='Metadata', index=False)
        return output.getvalue()
    
    def get_api_endpoints(self) -> Dict:
        return {
            'query': {
                'method': 'POST',
                'endpoint': '/api/query',
                'description': 'Execute OLAP query',
                'parameters': {
                    'cube': 'string',
                    'dimensions': 'array',
                    'measures': 'array',
                    'filters': 'object'
                }
            },
            'export': {
                'method': 'GET',
                'endpoint': '/api/export/{cube_name}',
                'description': 'Export cube data',
                'parameters': {'format': 'csv|excel|json|parquet'}
            },
            'metadata': {
                'method': 'GET',
                'endpoint': '/api/metadata/{cube_name}',
                'description': 'Get cube metadata'
            }
        }

# ============================================
# 9. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
class OLAPInterface:
    def __init__(self):
        self.conn = get_connection()
        self.olap_manager = OLAPManager(self.conn)
        self.user_manager = UserManager(self.conn)
        self.dashboard_manager = DashboardManager(self.olap_manager)
        self.api = OLAPAPI(self.olap_manager)
        
        if 'current_cube' not in st.session_state:
            st.session_state.current_cube = None
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'drill_path' not in st.session_state:
            st.session_state.drill_path = []
        if 'filters' not in st.session_state:
            st.session_state.filters = {}
        if 'pivot_rows' not in st.session_state:
            st.session_state.pivot_rows = []
        if 'pivot_cols' not in st.session_state:
            st.session_state.pivot_cols = []
        if 'pivot_measures' not in st.session_state:
            st.session_state.pivot_measures = []
    
    def render_login_page(self):
        st.markdown("<div class='login-container'>", unsafe_allow_html=True)
        st.markdown("## 🔐 Вход в систему")
        st.markdown("---")
        
        username = st.text_input("👤 Логин", placeholder="Введите логин")
        password = st.text_input("🔑 Пароль", type="password", placeholder="Введите пароль")
        
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
        st.markdown("<p style='text-align: center; color: #666;'>Демо-доступ: admin / admin123</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    
    def render_main_interface(self):
        st.markdown(f"""
        <div class='main-header'>
            <h1>🎲 OLAP Analytics Platform</h1>
            <p>Пользователь: {st.session_state.get('username', 'Guest')} 
            <span class='user-role-badge'>{st.session_state.get('role', 'VIEWER')}</span></p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.sidebar:
            st.markdown("## 🎯 Навигация")
            
            mode = st.radio(
                "Режим работы",
                ["📊 Анализ", "📈 Дашборды", "🏗️ Конструктор", "💾 Срезы", 
                 "⚙️ Администрирование", "🔌 API"]
            )
            
            if st.button("🚪 Выход", use_container_width=True):
                for key in ['authenticated', 'username', 'role', 'current_cube']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
            
            st.markdown("---")
            
            if st.checkbox("📊 Статистика кэша"):
                stats = self.olap_manager.query_cache.get_stats()
                st.metric("Размер кэша", f"{stats['size']} запросов")
                st.metric("Hit Rate", stats['hit_rate'])
                st.metric("Память", f"{stats['memory_usage']:.2f} MB")
                
                if st.button("🗑️ Очистить кэш", use_container_width=True):
                    self.olap_manager.query_cache.clear()
                    st.success("Кэш очищен")
                    st.rerun()
            
            st.markdown("---")
            
            try:
                cubes_df = self.conn.execute(
                    "SELECT name, updated_at FROM olap_cubes ORDER BY updated_at DESC"
                ).fetchdf()
                
                if not cubes_df.empty:
                    st.markdown("### 📦 Кубы")
                    selected_cube = st.selectbox("Выберите куб", cubes_df['name'].tolist())
                    
                    if st.button("🎲 Загрузить", use_container_width=True):
                        if self.user_manager.check_permission(selected_cube, 'READ'):
                            cube_def = self.conn.execute(
                                "SELECT definition, table_name FROM olap_cubes WHERE name = ?",
                                [selected_cube]
                            ).fetchone()
                            
                            if cube_def:
                                definition = json.loads(cube_def[0])
                                cube = OLAPCube(selected_cube, cube_def[1])
                                
                                for dim_name, dim_data in definition['dimensions'].items():
                                    dim = OLAPDimension(
                                        dim_name,
                                        dim_data['column'],
                                        dim_data.get('hierarchy', [])
                                    )
                                    cube.add_dimension(dim)
                                
                                for measure_name, measure_data in definition['measures'].items():
                                    measure = OLAPMeasure(
                                        measure_name,
                                        measure_data['column'],
                                        measure_data.get('default_agg', 'SUM')
                                    )
                                    cube.add_measure(measure)
                                
                                self.olap_manager.cubes[selected_cube] = cube
                                st.session_state.current_cube = cube
                                st.success(f"✅ Куб '{selected_cube}' загружен")
                                st.rerun()
                        else:
                            st.error("❌ Недостаточно прав")
            except:
                st.info("Нет доступных кубов. Создайте новый в Конструкторе.")
        
        if mode == "📊 Анализ":
            self.render_analysis_mode()
        elif mode == "📈 Дашборды":
            self.render_dashboard_mode()
        elif mode == "🏗️ Конструктор":
            self.render_cube_designer()
        elif mode == "💾 Срезы":
            self.render_slice_manager()
        elif mode == "⚙️ Администрирование":
            self.render_admin_panel()
        elif mode == "🔌 API":
            self.render_api_documentation()
    
    def render_analysis_mode(self):
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            
            tab1, tab2, tab3, tab4 = st.tabs([
                "🎯 Сводная таблица",
                "📊 Визуализации",
                "🔍 Drill-down",
                "⚡ Оптимизация"
            ])
            
            with tab1:
                self.render_pivot_table(cube)
            with tab2:
                self.render_visualizations(cube)
            with tab3:
                self.render_drill_down(cube)
            with tab4:
                self.render_optimization_panel(cube)
        else:
            st.info("👈 Выберите куб для анализа в боковом меню")
    
    def render_pivot_table(self, cube: OLAPCube):
        st.markdown("### 🎯 Интерактивная сводная таблица")
        
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
                export_format = st.selectbox("Экспорт", ["CSV", "Excel"])
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
                        st.download_button("📥 Скачать CSV", csv, "pivot.csv")
                    elif export_format == "Excel":
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            pivot_df.to_excel(writer, sheet_name='Pivot')
                        st.download_button("📥 Скачать Excel", output.getvalue(), "pivot.xlsx")
    
    def render_visualizations(self, cube: OLAPCube):
        st.markdown("### 📊 Визуализации")
        
        viz_type = st.selectbox("Тип визуализации", ["Treemap", "Waterfall", "Heatmap"])
        
        if viz_type == "Treemap":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            top_n = st.slider("Количество элементов", 5, 50, 20)
            
            if st.button("Создать Treemap"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_treemap(cube.name, dim, measure, top_n)
                    st.plotly_chart(fig, use_container_width=True)
        
        elif viz_type == "Waterfall":
            dim = st.selectbox("Измерение", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            
            if st.button("Создать Waterfall"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_waterfall(cube.name, dim, measure)
                    st.plotly_chart(fig, use_container_width=True)
        
        elif viz_type == "Heatmap":
            row_dim = st.selectbox("Строки", list(cube.dimensions.keys()))
            col_dim = st.selectbox("Колонки", list(cube.dimensions.keys()))
            measure = st.selectbox("Мера", list(cube.measures.keys()))
            
            if st.button("Создать Heatmap"):
                with st.spinner("Создание визуализации..."):
                    fig = self.dashboard_manager.create_heatmap(cube.name, row_dim, col_dim, measure)
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
                        SELECT 
                            DATE_PART('{next_level.lower()}', "{dim.column}") as {next_level},
                            COUNT(*) as count
                        FROM {cube.table_name}
                    """
                    
                    if st.session_state.drill_path:
                        for i, val in enumerate(st.session_state.drill_path):
                            level = dim.hierarchy[i].lower()
                            query += f" WHERE DATE_PART('{level}', \"{dim.column}\") = '{val}'"
                    
                    query += f" GROUP BY {next_level} ORDER BY {next_level}"
                    
                    df = self.conn.execute(query).fetchdf()
                    st.dataframe(df, use_container_width=True)
                    
                    if not df.empty:
                        selected_value = st.selectbox(f"Выберите {next_level}", df[next_level].tolist())
                        if st.button("Продолжить drill-down"):
                            st.session_state.drill_path.append(selected_value)
                            st.rerun()
            
            if st.session_state.drill_path:
                if st.button("⬆️ Drill up"):
                    st.session_state.drill_path.pop()
                    st.rerun()
        else:
            st.info("Нет измерений с иерархиями")
    
    def render_optimization_panel(self, cube: OLAPCube):
        st.markdown("### ⚡ Оптимизация производительности")
        
        st.markdown("#### 📊 Статистика запросов")
        stats_df = self.olap_manager.get_query_performance_stats()
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
        
        st.markdown("#### 💾 Материализованные представления")
        
        col1, col2 = st.columns(2)
        with col1:
            mv_dims = st.multiselect("Измерения", list(cube.dimensions.keys()), key="mv_dims")
        with col2:
            mv_measures = st.multiselect("Меры", list(cube.measures.keys()), key="mv_measures")
        
        if mv_measures:
            view_name = st.text_input("Название", f"MV_{datetime.now().strftime('%Y%m%d')}")
            if st.button("Создать"):
                table_name = self.olap_manager.create_materialized_view(
                    cube.name, view_name, mv_dims, mv_measures
                )
                st.success(f"✅ Создано: {table_name}")
    
    def render_dashboard_mode(self):
        st.markdown("### 📈 Интерактивные дашборды")
        
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            
            st.markdown("#### 🎯 Ключевые показатели")
            measures = list(cube.measures.keys())[:4]
            
            if measures:
                cols = st.columns(len(measures))
                kpis = self.dashboard_manager.create_kpi_cards(cube.name, measures)
                
                for i, (measure, values) in enumerate(kpis.items()):
                    with cols[i]:
                        st.metric(measure, f"{values['current']:,.0f}")
            
            st.markdown("#### 📊 Визуализации")
            chart_cols = st.columns(2)
            
            with chart_cols[0]:
                if len(cube.dimensions) > 0 and len(cube.measures) > 0:
                    dim = list(cube.dimensions.keys())[0]
                    measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_treemap(cube.name, dim, measure, 10)
                    st.plotly_chart(fig, use_container_width=True)
            
            with chart_cols[1]:
                if len(cube.dimensions) > 1 and len(cube.measures) > 0:
                    dim1 = list(cube.dimensions.keys())[0]
                    dim2 = list(cube.dimensions.keys())[1]
                    measure = list(cube.measures.keys())[0]
                    fig = self.dashboard_manager.create_heatmap(cube.name, dim1, dim2, measure)
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("👈 Выберите куб для просмотра дашборда")
    
    def render_cube_designer(self):
        st.markdown("### 🏗️ Конструктор OLAP кубов")
        
        if not self.user_manager.check_permission('*', 'WRITE'):
            st.error("❌ Недостаточно прав")
            return
        
        uploaded_file = st.file_uploader("Загрузите данные", type=['csv', 'xlsx', 'parquet'])
        
        if uploaded_file:
            cube_name = st.text_input("Название куба", f"Cube_{datetime.now().strftime('%Y%m%d')}")
            
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                elif uploaded_file.name.endswith('.parquet'):
                    df = pd.read_parquet(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.markdown("**Предпросмотр:**")
                st.dataframe(df.head(10), use_container_width=True)
                
                if st.button("🎲 Создать куб", type="primary"):
                    with st.spinner("Создание куба..."):
                        cube = self.olap_manager.create_cube_from_dataframe(cube_name, df, auto_detect=True)
                        st.session_state.current_cube = cube
                        st.success(f"✅ Куб '{cube_name}' создан!")
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Строк", len(df))
                        with col2:
                            st.metric("Измерений", len(cube.dimensions))
                        with col3:
                            st.metric("Мер", len(cube.measures))
            except Exception as e:
                st.error(f"Ошибка загрузки: {e}")
    
    def render_slice_manager(self):
        st.markdown("### 💾 Управление срезами данных")
        
        if st.session_state.current_cube:
            cube = st.session_state.current_cube
            
            st.markdown("#### 💾 Сохранить текущий срез")
            slice_name = st.text_input("Название среза")
            
            if st.button("Сохранить срез") and slice_name:
                slice_def = {
                    'cube': cube.name,
                    'filters': st.session_state.get('filters', {}),
                    'dimensions': st.session_state.get('pivot_rows', []) + st.session_state.get('pivot_cols', []),
                    'measures': st.session_state.get('pivot_measures', []),
                    'created_by': st.session_state.get('username'),
                    'timestamp': datetime.now().isoformat()
                }
                
                try:
                    max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM olap_slices").fetchone()[0]
                    self.conn.execute("""
                        INSERT INTO olap_slices (id, cube_name, slice_name, definition, created_at, owner)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """, [max_id + 1, cube.name, slice_name, json.dumps(slice_def), st.session_state.get('username')])
                    st.success("✅ Срез сохранен!")
                except Exception as e:
                    st.error(f"Ошибка: {e}")
            
            st.markdown("#### 📂 Сохраненные срезы")
            try:
                slices_df = self.conn.execute("""
                    SELECT id, slice_name, created_at, owner
                    FROM olap_slices 
                    WHERE cube_name = ?
                    ORDER BY created_at DESC
                """, [cube.name]).fetchdf()
                
                if not slices_df.empty:
                    st.dataframe(slices_df, use_container_width=True)
            except:
                st.info("Нет сохраненных срезов")
        else:
            st.info("👈 Выберите куб")
    
    def render_admin_panel(self):
        st.markdown("### ⚙️ Администрирование")
        
        if st.session_state.get('role') != 'ADMIN':
            st.error("❌ Доступ только для администраторов")
            return
        
        admin_tabs = st.tabs(["👥 Пользователи", "🔐 Права", "📊 Мониторинг"])
        
        with admin_tabs[0]:
            st.markdown("#### 👥 Управление пользователями")
            
            with st.expander("➕ Создать пользователя"):
                new_username = st.text_input("Логин")
                new_password = st.text_input("Пароль", type="password")
                new_role = st.selectbox("Роль", ["VIEWER", "ANALYST", "ADMIN"])
                
                if st.button("Создать"):
                    if self.user_manager.create_user(new_username, new_password, new_role):
                        st.success("✅ Пользователь создан")
                        st.rerun()
                    else:
                        st.error("❌ Ошибка")
            
            users_df = self.user_manager.get_users_list()
            if not users_df.empty:
                st.dataframe(users_df, use_container_width=True)
        
        with admin_tabs[1]:
            st.markdown("#### 🔐 Управление правами")
            
            try:
                cubes = self.conn.execute("SELECT name FROM olap_cubes").fetchdf()
                if not cubes.empty:
                    role = st.selectbox("Роль", ["VIEWER", "ANALYST"])
                    cube = st.selectbox("Куб", cubes['name'].tolist())
                    access = st.selectbox("Доступ", ["READ", "WRITE"])
                    
                    if st.button("Назначить"):
                        exists = self.conn.execute(
                            "SELECT COUNT(*) FROM permissions WHERE user_role = ? AND cube_name = ?",
                            [role, cube]
                        ).fetchone()[0]
                        
                        if exists > 0:
                            self.conn.execute(
                                "UPDATE permissions SET access_level = ? WHERE user_role = ? AND cube_name = ?",
                                [access, role, cube]
                            )
                        else:
                            max_id = self.conn.execute("SELECT COALESCE(MAX(id), 0) FROM permissions").fetchone()[0]
                            self.conn.execute(
                                "INSERT INTO permissions (id, user_role, cube_name, access_level) VALUES (?, ?, ?, ?)",
                                [max_id + 1, role, cube, access]
                            )
                        st.success("✅ Права назначены")
            except:
                st.info("Нет созданных кубов")
        
        with admin_tabs[2]:
            stats_df = self.olap_manager.get_query_performance_stats()
            if not stats_df.empty:
                st.dataframe(stats_df, use_container_width=True)
            
            cache_stats = self.olap_manager.query_cache.get_stats()
            st.json(cache_stats)
    
    def render_api_documentation(self):
        st.markdown("### 🔌 API для внешних систем")
        
        endpoints = self.api.get_api_endpoints()
        for name, endpoint in endpoints.items():
            with st.expander(f"{endpoint['method']} {endpoint['endpoint']}"):
                st.markdown(f"**Описание:** {endpoint['description']}")
                st.json(endpoint['parameters'])
        
        if st.session_state.current_cube:
            st.markdown("---")
            st.markdown("#### 🧪 Тестирование API")
            cube = st.session_state.current_cube
            
            if cube.measures and cube.dimensions:
                mdx_query = st.text_area(
                    "MDX Запрос",
                    f"SELECT {{Measures.[{list(cube.measures.keys())[0]}]}} ON COLUMNS,\n"
                    f"{{[Dimension].[{list(cube.dimensions.keys())[0]}]}} ON ROWS\n"
                    f"FROM [{cube.name}]"
                )
                
                if st.button("Выполнить MDX"):
                    result = self.api.execute_mdx_query(cube.name, mdx_query)
                    st.json(result)
            
            if st.button("📥 Экспорт для Power BI"):
                data = self.api.export_to_power_bi(st.session_state.current_cube.name)
                st.download_button(
                    "Скачать",
                    data,
                    f"{cube.name}_powerbi.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

# ============================================
# 10. ЗАПУСК
# ============================================
def main():
    interface = OLAPInterface()
    
    if not st.session_state.get('authenticated', False):
        interface.render_login_page()
    else:
        interface.render_main_interface()

if __name__ == "__main__":
    main()
