import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1. НАСТРОЙКА СТРАНИЦЫ И СТИЛИ
# ============================================
st.set_page_config(
    page_title="Ultimate Pivot Analytics",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Профессиональный CSS дизайн
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    .analytics-card {
        background: white;
        border-radius: 15px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
        transition: transform 0.3s;
    }
    .analytics-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 12px rgba(0,0,0,0.15);
    }
    .section-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 20px 0;
        font-size: 1.2em;
        font-weight: bold;
    }
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 10px 20px;
        font-weight: bold;
        transition: all 0.3s;
    }
    .stButton > button:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 10px 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    .stProgress > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .metric-card {
        background: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .favorite-star {
        color: gold;
        font-size: 24px;
        cursor: pointer;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 2. ГЛОБАЛЬНАЯ ФУНКЦИЯ ДЛЯ ПОДКЛЮЧЕНИЯ
# ============================================
@st.cache_resource
def get_duckdb_connection():
    """Создает подключение к DuckDB (кэшируется глобально)"""
    return duckdb.connect('analytics_cube.duckdb')

# ============================================
# 3. ВСПОМОГАТЕЛЬНЫЕ СТАТИСТИЧЕСКИЕ ФУНКЦИИ
# ============================================
def calculate_z_scores(data):
    """Расчет Z-score"""
    mean = np.mean(data)
    std = np.std(data)
    if std == 0:
        return np.zeros_like(data)
    return (data - mean) / std

def calculate_pearson_correlation(x, y):
    """Расчет корреляции Пирсона"""
    x = np.array(x)
    y = np.array(y)
    mask = ~(np.isnan(x) | np.isnan(y))
    x = x[mask]
    y = y[mask]
    if len(x) < 2:
        return 0
    n = len(x)
    sum_x = np.sum(x)
    sum_y = np.sum(y)
    sum_xy = np.sum(x * y)
    sum_x2 = np.sum(x * x)
    sum_y2 = np.sum(y * y)
    numerator = n * sum_xy - sum_x * sum_y
    denominator = np.sqrt((n * sum_x2 - sum_x**2) * (n * sum_y2 - sum_y**2))
    if denominator == 0:
        return 0
    return numerator / denominator

def calculate_p_value(r, n):
    """Расчет p-value для корреляции"""
    import math
    if abs(r) == 1:
        return 0
    t = r * math.sqrt((n - 2) / (1 - r**2))
    p_value = 2 * (1 - min(0.9999, abs(t) / (abs(t) + 10)))
    return p_value

# ============================================
# 4. УПРАВЛЕНИЕ ДАННЫМИ
# ============================================
class DataManager:
    def __init__(self, conn):
        self.conn = conn
        self.init_database()
    
    def init_database(self):
        """Инициализирует базу данных и все таблицы"""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS saved_reports (
                    id INTEGER PRIMARY KEY,
                    report_name VARCHAR,
                    report_date TIMESTAMP,
                    config VARCHAR,
                    data_hash VARCHAR,
                    is_favorite BOOLEAN
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS load_history (
                    id INTEGER PRIMARY KEY,
                    load_date TIMESTAMP,
                    file_name VARCHAR,
                    rows_loaded INTEGER,
                    status VARCHAR,
                    duration_seconds FLOAT
                )
            """)
        except Exception as e:
            st.error(f"Ошибка инициализации БД: {str(e)}")
    
    def load_excel_files(self, files, table_name='main_data'):
        """Загружает Excel файлы с прогрессом"""
        if not files:
            return 0, "Нет файлов"
        
        total_rows = 0
        first_file = True
        start_time = datetime.now()
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, file in enumerate(files):
            status_text.text(f"📂 Загрузка {file.name}... ({idx+1}/{len(files)})")
            
            try:
                df = pd.read_excel(file)
                
                # Оптимизация типов
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except:
                            pass
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except:
                            pass
                
                if first_file:
                    self.conn.register('temp_df', df)
                    self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
                    first_file = False
                else:
                    self.conn.register('temp_df', df)
                    self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                
                total_rows += len(df)
                
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'SUCCESS', ?)
                """, [file.name, len(df), 0])
                
            except Exception as e:
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'ERROR', ?)
                """, [file.name, 0, 0])
                st.error(f"Ошибка {file.name}: {str(e)}")
            
            progress_bar.progress((idx + 1) / len(files))
        
        duration = (datetime.now() - start_time).total_seconds()
        status_text.text(f"✅ Загружено {total_rows:,} строк за {duration:.1f} сек")
        progress_bar.empty()
        
        return total_rows, f"✅ Загружено {total_rows:,} строк из {len(files)} файлов"
    
    def add_new_files(self, files, table_name='main_data'):
        """Добавляет новые файлы к существующим данным"""
        if not files:
            return 0, "Нет файлов"
        
        total_rows = 0
        
        for file in files:
            try:
                df = pd.read_excel(file)
                self.conn.register('temp_df', df)
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                total_rows += len(df)
                
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'APPEND', 0)
                """, [file.name, len(df)])
                
            except Exception as e:
                st.error(f"Ошибка добавления {file.name}: {str(e)}")
        
        return total_rows, f"✅ Добавлено {total_rows:,} строк"
    
    def get_data_info(self, table_name='main_data'):
        """Получает полную информацию о данных"""
        try:
            tables = self.conn.execute("SHOW TABLES").fetchdf()
            if len(tables) == 0 or table_name not in tables['name'].values:
                return None
            
            total_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if total_rows == 0:
                return None
            
            df_sample = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 10000").fetchdf()
            
            info = {
                'total_rows': total_rows,
                'total_columns': len(df_sample.columns),
                'dimensions': [],
                'metrics': [],
                'dates': [],
                'memory_mb': df_sample.memory_usage(deep=True).sum() / 1024 / 1024
            }
            
            for col in df_sample.columns:
                col_info = {
                    'name': col,
                    'dtype': str(df_sample[col].dtype),
                    'unique_count': df_sample[col].nunique(),
                    'null_count': df_sample[col].isnull().sum(),
                    'null_percentage': (df_sample[col].isnull().sum() / len(df_sample)) * 100
                }
                
                if pd.api.types.is_numeric_dtype(df_sample[col]):
                    col_info['category'] = 'metric'
                    col_info['stats'] = {
                        'min': float(df_sample[col].min()) if not df_sample[col].isnull().all() else None,
                        'max': float(df_sample[col].max()) if not df_sample[col].isnull().all() else None,
                        'mean': float(df_sample[col].mean()) if not df_sample[col].isnull().all() else None,
                        'median': float(df_sample[col].median()) if not df_sample[col].isnull().all() else None,
                        'std': float(df_sample[col].std()) if not df_sample[col].isnull().all() else None
                    }
                    info['metrics'].append(col_info)
                elif pd.api.types.is_datetime64_any_dtype(df_sample[col]):
                    col_info['category'] = 'date'
                    info['dates'].append(col_info)
                else:
                    col_info['category'] = 'dimension'
                    info['dimensions'].append(col_info)
            
            return info
        except Exception as e:
            return None

# ============================================
# 5. РАСШИРЕННАЯ АНАЛИТИКА
# ============================================
class AdvancedAnalytics:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def time_series_analysis(self, date_column, metric_column, freq='M'):
        """Временной ряд анализ с трендами и сезонностью"""
        try:
            query = f"""
                SELECT 
                    DATE_TRUNC('{freq}', {date_column}) as period,
                    SUM({metric_column}) as total,
                    AVG({metric_column}) as avg,
                    COUNT(*) as count
                FROM {self.table_name}
                GROUP BY period
                ORDER BY period
            """
            df = self.conn.execute(query).fetchdf()
            
            if len(df) > 1:
                df['moving_avg_3'] = df['total'].rolling(window=3, min_periods=1).mean()
                df['moving_avg_7'] = df['total'].rolling(window=7, min_periods=1).mean()
                df['growth'] = df['total'].pct_change() * 100
                
                if len(df) > 2:
                    x = np.arange(len(df))
                    z = np.polyfit(x, df['total'].fillna(0), 1)
                    df['trend'] = np.polyval(z, x)
            
            return df
        except Exception as e:
            return None
    
    def correlation_analysis(self, metrics):
        """Корреляционный анализ между метриками"""
        if len(metrics) < 2:
            return None
        
        try:
            query = f"SELECT {', '.join(metrics)} FROM {self.table_name} LIMIT 100000"
            df = self.conn.execute(query).fetchdf()
            
            corr_matrix = pd.DataFrame(index=metrics, columns=metrics)
            p_values = pd.DataFrame(index=metrics, columns=metrics)
            
            for i, m1 in enumerate(metrics):
                for j, m2 in enumerate(metrics):
                    if i == j:
                        corr_matrix.loc[m1, m2] = 1
                        p_values.loc[m1, m2] = 0
                    else:
                        r = calculate_pearson_correlation(df[m1].values, df[m2].values)
                        corr_matrix.loc[m1, m2] = r
                        n = len(df[m1].dropna())
                        p_values.loc[m1, m2] = calculate_p_value(r, n)
            
            return {
                'correlation': corr_matrix.astype(float),
                'p_values': p_values.astype(float)
            }
        except Exception as e:
            return None
    
    def predictive_forecast(self, date_column, metric_column, periods=12):
        """Прогноз на основе линейной регрессии"""
        try:
            query = f"""
                SELECT 
                    DATE_TRUNC('month', {date_column}) as period,
                    SUM({metric_column}) as value
                FROM {self.table_name}
                GROUP BY period
                ORDER BY period
                LIMIT 100
            """
            df = self.conn.execute(query).fetchdf()
            
            if len(df) < 3:
                return None
            
            x = np.arange(len(df))
            y = df['value'].values
            
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            
            future_x = np.arange(len(df), len(df) + periods)
            forecast = p(future_x)
            
            residuals = y - p(x)
            std_residuals = np.std(residuals)
            upper_bound = forecast + 1.96 * std_residuals
            lower_bound = forecast - 1.96 * std_residuals
            
            return {
                'historical': df,
                'forecast': forecast,
                'upper_bound': upper_bound,
                'lower_bound': lower_bound
            }
        except Exception as e:
            return None
    
    def anomaly_detection(self, metric_column, threshold=3):
        """Обнаружение аномалий с использованием Z-score и IQR"""
        try:
            query = f"SELECT {metric_column} FROM {self.table_name}"
            values = self.conn.execute(query).fetchdf()[metric_column].values
            
            z_scores = calculate_z_scores(values)
            z_anomalies = np.where(np.abs(z_scores) > threshold)[0]
            
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            iqr_anomalies = np.where((values < (q1 - 1.5 * iqr)) | (values > (q3 + 1.5 * iqr)))[0]
            
            return {
                'z_score_anomalies': z_anomalies,
                'iqr_anomalies': iqr_anomalies,
                'z_scores': z_scores,
                'values': values,
                'threshold': threshold
            }
        except Exception as e:
            return None
    
    def cohort_analysis(self, dimension, metric, cohort_period='month'):
        """Когортный анализ"""
        try:
            query = f"""
                WITH cohorts AS (
                    SELECT 
                        {dimension} as cohort,
                        DATE_TRUNC('{cohort_period}', MIN(date)) as cohort_date
                    FROM {self.table_name}
                    GROUP BY {dimension}
                )
                SELECT 
                    c.cohort,
                    c.cohort_date,
                    SUM(t.{metric}) as total_value
                FROM {self.table_name} t
                JOIN cohorts c ON t.{dimension} = c.cohort
                GROUP BY c.cohort, c.cohort_date
                ORDER BY c.cohort_date
            """
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            return None
    
    def segmentation(self, segment_column, metrics):
        """Сегментационный анализ"""
        try:
            segment_stats = []
            for metric in metrics:
                query = f"""
                    SELECT 
                        {segment_column},
                        COUNT(*) as count,
                        AVG({metric}) as avg_{metric},
                        SUM({metric}) as total_{metric},
                        MIN({metric}) as min_{metric},
                        MAX({metric}) as max_{metric},
                        STDDEV({metric}) as std_{metric}
                    FROM {self.table_name}
                    GROUP BY {segment_column}
                    ORDER BY total_{metric} DESC
                    LIMIT 20
                """
                stats = self.conn.execute(query).fetchdf()
                segment_stats.append(stats)
            return segment_stats
        except Exception as e:
            return None

# ============================================
# 6. УМНАЯ СВОДНАЯ ТАБЛИЦА
# ============================================
class SmartPivotTable:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def create_pivot(self, rows, columns, values, agg_func='SUM', 
                     show_totals=True, show_percentages=False, 
                     rank_values=False):
        """Создает расширенную сводную таблицу"""
        
        if not values:
            try:
                info = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 1").fetchdf()
                metrics = [col for col in info.columns if pd.api.types.is_numeric_dtype(info[col])]
                if metrics:
                    values = [metrics[0]]
                else:
                    return None, "Нет числовых полей для анализа"
            except:
                return None, "Нет данных для анализа"
        
        try:
            group_by = rows + columns
            
            if group_by:
                agg_exprs = [f"{agg_func}({v}) as {v}" for v in values]
                query = f"""
                    SELECT 
                        {', '.join(group_by)},
                        {', '.join(agg_exprs)}
                    FROM {self.table_name}
                    GROUP BY {', '.join(group_by)}
                    ORDER BY {', '.join(group_by)}
                    LIMIT 50000
                """
                
                result = self.conn.execute(query).fetchdf()
                
                if len(result) == 0:
                    return None, "Нет данных для отображения"
                
                if columns and len(columns) > 0:
                    if len(values) == 1:
                        pivot_df = result.pivot_table(
                            index=rows if rows else None,
                            columns=columns,
                            values=values[0],
                            aggfunc=agg_func.lower(),
                            fill_value=0
                        )
                    else:
                        pivot_dfs = []
                        for value in values:
                            temp_pivot = result.pivot_table(
                                index=rows if rows else None,
                                columns=columns,
                                values=value,
                                aggfunc=agg_func.lower(),
                                fill_value=0
                            )
                            temp_pivot.columns = [f"{value} - {col}" for col in temp_pivot.columns]
                            pivot_dfs.append(temp_pivot)
                        pivot_df = pd.concat(pivot_dfs, axis=1)
                    
                    if show_percentages and len(pivot_df) > 0:
                        total_sum = pivot_df.sum().sum()
                        if total_sum > 0:
                            pivot_df = (pivot_df / total_sum) * 100
                    
                    if rank_values and len(pivot_df) > 0:
                        for col in pivot_df.columns:
                            if pd.api.types.is_numeric_dtype(pivot_df[col]):
                                pivot_df[f"{col}_rank"] = pivot_df[col].rank(ascending=False)
                    
                    if show_totals:
                        pivot_df.loc['ИТОГО по строкам'] = pivot_df.sum()
                        pivot_df['ИТОГО по колонкам'] = pivot_df.sum(axis=1)
                    
                    return pivot_df, f"✅ Сводная таблица: {len(pivot_df)} строк × {len(pivot_df.columns)} столбцов"
                else:
                    return result, f"✅ Группировка: {len(result)} строк"
            else:
                agg_exprs = [f"{agg_func}({v}) as {v}" for v in values]
                query = f"SELECT {', '.join(agg_exprs)} FROM {self.table_name}"
                result = self.conn.execute(query).fetchdf()
                return result, "✅ Агрегированные данные"
                
        except Exception as e:
            return None, f"❌ Ошибка: {str(e)}"
    
    def create_dashboard(self, rows, columns, values, agg_func='SUM'):
        """Создает аналитическую панель"""
        pivot_df, message = self.create_pivot(rows, columns, values, agg_func, 
                                              show_totals=True, show_percentages=True)
        
        if pivot_df is None:
            return None, message
        
        dashboard = {
            'data': pivot_df,
            'summary': {
                'total_rows': len(pivot_df),
                'total_columns': len(pivot_df.columns),
                'total_sum': pivot_df.select_dtypes(include=['number']).sum().sum(),
                'average': pivot_df.select_dtypes(include=['number']).mean().mean(),
                'max_value': pivot_df.select_dtypes(include=['number']).max().max(),
                'min_value': pivot_df.select_dtypes(include=['number']).min().min()
            }
        }
        
        return dashboard, message

# ============================================
# 7. УПРАВЛЕНИЕ ОТЧЕТАМИ
# ============================================
class ReportManager:
    def __init__(self, conn):
        self.conn = conn
    
    def save_report(self, name, config, data):
        """Сохраняет отчет"""
        data_hash = hashlib.md5(data.to_json().encode()).hexdigest()
        
        self.conn.execute("""
            INSERT INTO saved_reports (report_name, report_date, config, data_hash, is_favorite)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
        """, [name, json.dumps(config), data_hash, False])
        
        return True
    
    def load_reports(self, favorite_only=False):
        """Загружает сохраненные отчеты"""
        query = "SELECT * FROM saved_reports"
        if favorite_only:
            query += " WHERE is_favorite = TRUE"
        query += " ORDER BY report_date DESC"
        
        return self.conn.execute(query).fetchdf()
    
    def delete_report(self, report_id):
        """Удаляет отчет"""
        self.conn.execute("DELETE FROM saved_reports WHERE id = ?", [report_id])
    
    def toggle_favorite(self, report_id, is_favorite):
        """Переключает статус избранного"""
        self.conn.execute("UPDATE saved_reports SET is_favorite = ? WHERE id = ?", 
                         [is_favorite, report_id])

# ============================================
# 8. ВИЗУАЛИЗАЦИИ
# ============================================
def create_advanced_charts(df, chart_type, metric, rows_limit=50):
    """Создает расширенные визуализации"""
    
    if df.empty or metric not in df.columns:
        return None
    
    plot_data = df[metric].head(rows_limit)
    
    if chart_type == "📊 Столбчатая":
        fig = px.bar(
            x=plot_data.index,
            y=plot_data.values,
            title=f"{metric} - Столбчатая диаграмма",
            labels={'x': 'Категория', 'y': metric},
            color_discrete_sequence=['#FF4B4B'],
            text=plot_data.values
        )
        fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
        
    elif chart_type == "📈 Линейная":
        fig = px.line(
            x=plot_data.index,
            y=plot_data.values,
            title=f"{metric} - Тренд",
            markers=True,
            line_shape='linear'
        )
        fig.update_traces(line=dict(width=3), marker=dict(size=8))
        
    elif chart_type == "🥧 Круговая":
        plot_data = plot_data.head(10)
        fig = px.pie(
            values=plot_data.values,
            names=plot_data.index,
            title=f"Распределение {metric}",
            hole=0.3
        )
        
    elif chart_type == "🎯 Тепловая карта":
        if len(df.columns) > 1 and len(df) > 1:
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 1:
                fig = px.imshow(
                    df[numeric_cols].values,
                    x=numeric_cols,
                    y=df.index,
                    title="Тепловая карта данных",
                    color_continuous_scale='Viridis',
                    aspect="auto"
                )
            else:
                fig = None
        else:
            fig = None
            
    elif chart_type == "📦 Ящик с усами":
        fig = px.box(
            y=plot_data.values,
            title=f"Распределение {metric}",
            points="all"
        )
        
    else:
        fig = None
    
    return fig

# ============================================
# 9. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
def main():
    # Инициализация
    conn = get_duckdb_connection()
    data_manager = DataManager(conn)
    analytics = AdvancedAnalytics(conn)
    pivot_tool = SmartPivotTable(conn)
    report_manager = ReportManager(conn)
    
    # Заголовок
    st.title("🚀 Ultimate Pivot Analytics Platform")
    st.markdown("*Профессиональная аналитика как в Excel, но мощнее*")
    
    # ============================================
    # САЙДБАР
    # ============================================
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/pivot-table.png", width=80)
        st.markdown("## 📊 Управление")
        
        # Вкладки в сайдбаре
        sidebar_tab = st.radio(
            "Меню",
            ["📁 Данные", "💾 Отчеты", "⭐ Избранное", "⚙️ Настройки"],
            index=0
        )
        
        if sidebar_tab == "📁 Данные":
            st.markdown("### Загрузка файлов")
            
            uploaded_files = st.file_uploader(
                "Выберите Excel файлы",
                type=['xlsx', 'xls', 'csv'],
                accept_multiple_files=True,
                key="file_uploader"
            )
            
            if uploaded_files:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🚀 Новая загрузка", use_container_width=True):
                        with st.spinner("Загрузка..."):
                            rows, msg = data_manager.load_excel_files(uploaded_files)
                            st.success(msg)
                            st.rerun()
                
                with col2:
                    if st.button("➕ Добавить файлы", use_container_width=True):
                        with st.spinner("Добавление..."):
                            rows, msg = data_manager.add_new_files(uploaded_files)
                            st.success(msg)
                            st.rerun()
            
            st.divider()
            
            # Информация о данных
            data_info = data_manager.get_data_info()
            if data_info:
                st.markdown("### 📈 Статистика данных")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Строк", f"{data_info['total_rows']:,}")
                with col2:
                    st.metric("Колонок", data_info['total_columns'])
                with col3:
                    st.metric("Память", f"{data_info['memory_mb']:.1f} MB")
                
                with st.expander("📋 Структура данных"):
                    col_data = []
                    for col in data_info['dimensions'][:5]:
                        col_data.append({'Колонка': col['name'], 'Тип': '📐 Измерение', 'Уникальных': col['unique_count']})
                    for col in data_info['metrics'][:5]:
                        col_data.append({'Колонка': col['name'], 'Тип': '💹 Метрика', 'Уникальных': col['unique_count']})
                    st.dataframe(pd.DataFrame(col_data), use_container_width=True)
        
        elif sidebar_tab == "💾 Отчеты":
            st.markdown("### Сохраненные отчеты")
            
            reports = report_manager.load_reports()
            if not reports.empty:
                for _, report in reports.iterrows():
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**📄 {report['report_name']}**")
                            st.caption(report['report_date'].strftime("%Y-%m-%d %H:%M"))
                        with col2:
                            if st.button("⭐" if report['is_favorite'] else "☆", key=f"fav_{report['id']}"):
                                report_manager.toggle_favorite(report['id'], not report['is_favorite'])
                                st.rerun()
                        st.divider()
            else:
                st.info("Нет сохраненных отчетов")
        
        elif sidebar_tab == "⭐ Избранное":
            st.markdown("### Избранные отчеты")
            
            favorites = report_manager.load_reports(favorite_only=True)
            if not favorites.empty:
                for _, fav in favorites.iterrows():
                    st.markdown(f"**⭐ {fav['report_name']}**")
                    st.caption(fav['report_date'].strftime("%Y-%m-%d %H:%M"))
                    st.divider()
            else:
                st.info("Нет избранных отчетов")
        
        else:  # Настройки
            st.markdown("### ⚙️ Настройки")
            
            theme = st.selectbox("Тема", ["Светлая", "Темная", "Системная"])
            language = st.selectbox("Язык", ["Русский", "English"])
            default_agg = st.selectbox("Агрегация по умолчанию", ["SUM", "COUNT", "AVG", "MIN", "MAX"])
            
            st.divider()
            
            if st.button("🗑️ Очистить все данные", use_container_width=True):
                try:
                    conn.execute("DROP TABLE IF EXISTS main_data")
                    st.success("Данные очищены")
                    st.rerun()
                except:
                    pass
    
    # ============================================
    # ОСНОВНАЯ ОБЛАСТЬ
    # ============================================
    
    data_info = data_manager.get_data_info()
    
    if data_info and data_info['total_rows'] > 0:
        # Главные вкладки
        main_tab1, main_tab2, main_tab3, main_tab4, main_tab5 = st.tabs([
            "📊 Сводная таблица", 
            "📈 Расширенная аналитика", 
            "🎨 Визуализации",
            "🔮 Прогнозы и тренды",
            "💾 Сохранение"
        ])
        
        with main_tab1:
            st.markdown('<div class="section-header">📊 Конструктор сводной таблицы</div>', unsafe_allow_html=True)
            
            # Три колонки для настроек
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 📊 СТРОКИ")
                dimension_names = [d['name'] for d in data_info['dimensions']] + [d['name'] for d in data_info['dates']]
                rows = st.multiselect("Поля для строк", dimension_names, key="rows", placeholder="Выберите поля...")
                if rows:
                    st.caption(f"✅ {len(rows)} полей выбрано")
            
            with col2:
                st.markdown("### 📈 КОЛОНКИ")
                columns = st.multiselect("Поля для колонок", dimension_names, key="columns", placeholder="Выберите поля...")
                if columns:
                    st.caption(f"✅ {len(columns)} полей выбрано")
            
            with col3:
                st.markdown("### 🧮 ЗНАЧЕНИЯ")
                metric_names = [m['name'] for m in data_info['metrics']]
                values = st.multiselect("Числовые поля", metric_names, key="values", placeholder="Выберите метрики...")
                
                if values:
                    agg_func = st.selectbox("Агрегация", ["SUM", "COUNT", "AVG", "MIN", "MAX", "STD"], key="agg_func")
                    st.caption(f"✅ {len(values)} метрик выбрано")
                else:
                    st.info("💡 Будут выбраны автоматически")
            
            # Дополнительные опции
            with st.expander("⚙️ Расширенные настройки"):
                col_opt1, col_opt2, col_opt3 = st.columns(3)
                with col_opt1:
                    show_totals = st.checkbox("Показывать итоги", True)
                with col_opt2:
                    show_percentages = st.checkbox("Показывать проценты", False)
                with col_opt3:
                    rank_values = st.checkbox("Ранжировать значения", False)
            
            # Кнопка построения
            if st.button("🔄 ПОСТРОИТЬ СВОДНУЮ ТАБЛИЦУ", type="primary", use_container_width=True):
                with st.spinner("🔨 Построение сводной таблицы..."):
                    result, msg = pivot_tool.create_pivot(
                        rows, columns, values, agg_func if values else 'SUM',
                        show_totals, show_percentages, rank_values
                    )
                    
                    if result is not None:
                        st.success(msg)
                        st.session_state['pivot_result'] = result
                        
                        # Показываем результат
                        st.dataframe(result.style.background_gradient(cmap='Blues'), 
                                    use_container_width=True, height=500)
                        
                        # Быстрая статистика
                        st.markdown("### 📊 Краткая статистика")
                        col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                        with col_stat1:
                            st.metric("Строк", f"{len(result):,}")
                        with col_stat2:
                            st.metric("Колонок", f"{len(result.columns):,}")
                        with col_stat3:
                            total = result.select_dtypes(include=['number']).sum().sum()
                            st.metric("Общая сумма", f"{total:,.0f}")
                        with col_stat4:
                            avg = result.select_dtypes(include=['number']).mean().mean()
                            st.metric("Среднее", f"{avg:,.2f}")
                        
                        # Экспорт результата
                        col_exp1, col_exp2 = st.columns(2)
                        with col_exp1:
                            csv = result.to_csv()
                            st.download_button("📥 Скачать CSV", csv, "pivot_table.csv", "text/csv")
                        with col_exp2:
                            if st.button("💾 Сохранить отчет"):
                                report_manager.save_report(f"Отчет_{datetime.now().strftime('%Y%m%d_%H%M%S')}", 
                                                          {'rows': rows, 'columns': columns, 'values': values}, result)
                                st.success("Отчет сохранен!")
                    else:
                        st.error(msg)
        
        with main_tab2:
            st.markdown('<div class="section-header">📈 Расширенный аналитический модуль</div>', unsafe_allow_html=True)
            
            analytics_type = st.selectbox(
                "Тип анализа",
                ["Корреляционный анализ", "Когортный анализ", "Сегментация", "Обнаружение аномалий", "Временные ряды"]
            )
            
            if analytics_type == "Корреляционный анализ":
                metric_names = [m['name'] for m in data_info['metrics']]
                selected_metrics = st.multiselect("Выберите метрики для корреляции", metric_names, 
                                                  default=metric_names[:min(3, len(metric_names))])
                if len(selected_metrics) >= 2:
                    corr_result = analytics.correlation_analysis(selected_metrics)
                    if corr_result:
                        st.subheader("📊 Корреляционная матрица")
                        fig = px.imshow(
                            corr_result['correlation'],
                            text_auto=True,
                            title="Корреляция между метриками",
                            color_continuous_scale='RdBu',
                            zmin=-1, zmax=1,
                            aspect="auto"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Сильные корреляции
                        st.subheader("🔗 Сильные корреляции")
                        strong_corr = []
                        for i in range(len(selected_metrics)):
                            for j in range(i+1, len(selected_metrics)):
                                r = corr_result['correlation'].iloc[i, j]
                                if abs(r) > 0.5:
                                    strong_corr.append({
                                        'Метрика 1': selected_metrics[i],
                                        'Метрика 2': selected_metrics[j],
                                        'Корреляция': r,
                                        'Сила': 'Сильная положительная' if r > 0 else 'Сильная отрицательная'
                                    })
                        if strong_corr:
                            st.dataframe(pd.DataFrame(strong_corr), use_container_width=True)
            
            elif analytics_type == "Обнаружение аномалий":
                metric_names = [m['name'] for m in data_info['metrics']]
                metric_for_anomaly = st.selectbox("Выберите метрику для анализа", metric_names)
                threshold = st.slider("Порог чувствительности", 1, 5, 3, help="Больше = меньше аномалий")
                
                if st.button("🔍 Найти аномалии"):
                    anomalies = analytics.anomaly_detection(metric_for_anomaly, threshold)
                    if anomalies:
                        st.success(f"🔴 Найдено аномалий: {len(anomalies['z_score_anomalies'])} (Z-score) и {len(anomalies['iqr_anomalies'])} (IQR)")
                        
                        # Визуализация
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            y=anomalies['values'], 
                            mode='lines+markers', 
                            name='Значения',
                            line=dict(color='blue', width=2),
                            marker=dict(size=6)
                        ))
                        
                        # Отмечаем аномалии
                        if len(anomalies['z_score_anomalies']) > 0:
                            anomaly_values = anomalies['values'][anomalies['z_score_anomalies']]
                            fig.add_trace(go.Scatter(
                                y=anomaly_values,
                                mode='markers',
                                name='Аномалии',
                                marker=dict(color='red', size=12, symbol='x', line=dict(width=2))
                            ))
                        
                        fig.update_layout(
                            title=f"Обнаружение аномалий в {metric_for_anomaly}",
                            xaxis_title="Индекс",
                            yaxis_title=metric_for_anomaly,
                            hovermode='x unified',
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Статистика аномалий
                        if len(anomalies['z_score_anomalies']) > 0:
                            st.subheader("📊 Статистика аномалий")
                            anomaly_values = anomalies['values'][anomalies['z_score_anomalies']]
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Кол-во аномалий", len(anomalies['z_score_anomalies']))
                            with col2:
                                st.metric("Среднее значение аномалий", f"{np.mean(anomaly_values):.2f}")
                            with col3:
                                st.metric("Максимальная аномалия", f"{np.max(anomaly_values):.2f}")
            
            elif analytics_type == "Временные ряды":
                if data_info['dates']:
                    date_col = st.selectbox("Колонка с датой", [d['name'] for d in data_info['dates']])
                    metric_col = st.selectbox("Метрика", [m['name'] for m in data_info['metrics']])
                    freq = st.selectbox("Период агрегации", ["day", "week", "month", "quarter"])
                    freq_map = {"day": "day", "week": "week", "month": "month", "quarter": "quarter"}
                    
                    ts_data = analytics.time_series_analysis(date_col, metric_col, freq_map[freq])
                    if ts_data is not None and len(ts_data) > 0:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=ts_data['period'],
                            y=ts_data['total'],
                            mode='lines+markers',
                            name='Факт',
                            line=dict(color='blue', width=2)
                        ))
                        
                        if 'moving_avg_3' in ts_data.columns:
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['moving_avg_3'],
                                mode='lines',
                                name='Скользящая средняя (3)',
                                line=dict(color='orange', width=2, dash='dash')
                            ))
                        
                        if 'trend' in ts_data.columns:
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['trend'],
                                mode='lines',
                                name='Тренд',
                                line=dict(color='green', width=2, dash='dot')
                            ))
                        
                        fig.update_layout(
                            title=f"Временной ряд: {metric_col}",
                            xaxis_title="Период",
                            yaxis_title=metric_col,
                            hovermode='x unified',
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Рост/падение
                        if 'growth' in ts_data.columns:
                            st.subheader("📈 Динамика роста")
                            growth_data = ts_data[['period', 'growth']].dropna()
                            fig_growth = px.bar(growth_data, x='period', y='growth', 
                                               title="Процент роста/падения",
                                               color='growth',
                                               color_continuous_scale='RdYlGn')
                            st.plotly_chart(fig_growth, use_container_width=True)
        
        with main_tab3:
            st.markdown('<div class="section-header">🎨 Интерактивные визуализации</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Тип визуализации",
                ["📊 Столбчатая диаграмма", "📈 Линейный график", "🥧 Круговая диаграмма", 
                 "📦 Ящик с усами", "🎯 Тепловая карта", "☀️ Солнечные лучи"]
            )
            
            col_viz1, col_viz2 = st.columns(2)
            
            with col_viz1:
                if data_info['dimensions']:
                    x_axis = st.selectbox("Ось X (Категория)", [d['name'] for d in data_info['dimensions']])
                else:
                    x_axis = None
                    st.warning("Нет категориальных полей")
            
            with col_viz2:
                y_axis = st.selectbox("Ось Y (Значение)", [m['name'] for m in data_info['metrics']])
            
            if st.button("🎨 Построить график", use_container_width=True):
                if x_axis:
                    query = f"SELECT {x_axis}, SUM({y_axis}) as total FROM main_data GROUP BY {x_axis} ORDER BY total DESC LIMIT 20"
                    plot_data = conn.execute(query).fetchdf()
                    
                    if viz_type == "📊 Столбчатая диаграмма":
                        fig = px.bar(plot_data, x=x_axis, y='total', 
                                    title=f"{y_axis} по {x_axis}",
                                    color='total',
                                    color_continuous_scale='Viridis',
                                    text='total')
                        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "📈 Линейный график":
                        fig = px.line(plot_data, x=x_axis, y='total', 
                                     title=f"Тренд {y_axis}",
                                     markers=True)
                        fig.update_traces(line=dict(width=3), marker=dict(size=8))
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "🥧 Круговая диаграмма":
                        fig = px.pie(plot_data.head(10), values='total', names=x_axis, 
                                    title=f"Распределение {y_axis}",
                                    hole=0.3)
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "📦 Ящик с усами":
                        fig = px.box(plot_data, y='total', title=f"Распределение {y_axis}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "🎯 Тепловая карта":
                        # Создаем матрицу для тепловой карты
                        if len(data_info['dimensions']) >= 2:
                            dim2 = st.selectbox("Второе измерение", [d['name'] for d in data_info['dimensions'] if d['name'] != x_axis])
                            heat_query = f"""
                                SELECT {x_axis}, {dim2}, SUM({y_axis}) as value
                                FROM main_data
                                GROUP BY {x_axis}, {dim2}
                            """
                            heat_data = conn.execute(heat_query).fetchdf()
                            heat_pivot = heat_data.pivot(index=x_axis, columns=dim2, values='value').fillna(0)
                            fig = px.imshow(heat_pivot, text_auto=True, aspect="auto",
                                          title=f"Тепловая карта: {y_axis}")
                            st.plotly_chart(fig, use_container_width=True)
                    
                    elif viz_type == "☀️ Солнечные лучи":
                        if len(data_info['dimensions']) >= 2:
                            dim2 = st.selectbox("Второй уровень", [d['name'] for d in data_info['dimensions'] if d['name'] != x_axis])
                            sun_query = f"""
                                SELECT {x_axis}, {dim2}, SUM({y_axis}) as value
                                FROM main_data
                                GROUP BY {x_axis}, {dim2}
                            """
                            sun_data = conn.execute(sun_query).fetchdf()
                            fig = px.sunburst(sun_data, path=[x_axis, dim2], values='value',
                                            title=f"Иерархия {y_axis}")
                            st.plotly_chart(fig, use_container_width=True)
        
        with main_tab4:
            st.markdown('<div class="section-header">🔮 Прогнозирование и тренды</div>', unsafe_allow_html=True)
            
            if data_info['dates']:
                date_col = st.selectbox("Колонка с датой", [d['name'] for d in data_info['dates']])
                metric_for_forecast = st.selectbox("Метрика для прогноза", [m['name'] for m in data_info['metrics']])
                periods = st.slider("Периодов прогноза", 3, 24, 12, help="Количество месяцев для прогноза")
                
                if st.button("🔮 Построить прогноз", type="primary", use_container_width=True):
                    forecast = analytics.predictive_forecast(date_col, metric_for_forecast, periods)
                    
                    if forecast:
                        fig = go.Figure()
                        
                        # Исторические данные
                        fig.add_trace(go.Scatter(
                            x=forecast['historical']['period'],
                            y=forecast['historical']['value'],
                            mode='lines+markers',
                            name='Исторические данные',
                            line=dict(color='blue', width=3),
                            marker=dict(size=8)
                        ))
                        
                        # Прогноз
                        last_date = forecast['historical']['period'].iloc[-1]
                        future_dates = [last_date + timedelta(days=30*i) for i in range(1, periods+1)]
                        
                        fig.add_trace(go.Scatter(
                            x=future_dates,
                            y=forecast['forecast'],
                            mode='lines+markers',
                            name='Прогноз',
                            line=dict(color='red', width=3, dash='dash'),
                            marker=dict(size=8, symbol='diamond')
                        ))
                        
                        # Доверительный интервал
                        fig.add_trace(go.Scatter(
                            x=future_dates + future_dates[::-1],
                            y=list(forecast['upper_bound']) + list(forecast['lower_bound'][::-1]),
                            fill='toself',
                            fillcolor='rgba(255,0,0,0.2)',
                            line=dict(color='rgba(255,255,255,0)'),
                            name='Доверительный интервал 95%'
                        ))
                        
                        fig.update_layout(
                            title=f"📈 Прогноз {metric_for_forecast} на {periods} периодов",
                            xaxis_title="Период",
                            yaxis_title=metric_for_forecast,
                            hovermode='x unified',
                            height=500
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Метрики качества прогноза
                        st.subheader("📊 Метрики прогноза")
                        col1, col2, col3, col4 = st.columns(4)
                        
                        historical_values = forecast['historical']['value']
                        with col1:
                            st.metric("Среднее значение", f"{historical_values.mean():.2f}")
                        with col2:
                            st.metric("Тренд", f"{forecast['forecast'][-1] - historical_values.iloc[-1]:.2f}")
                        with col3:
                            st.metric("Волатильность", f"{historical_values.std():.2f}")
                        with col4:
                            growth = ((forecast['forecast'][-1] - historical_values.iloc[-1]) / historical_values.iloc[-1]) * 100
                            st.metric("Прогнозируемый рост", f"{growth:.1f}%")
                        
                        # Таблица прогноза
                        st.subheader("📋 Детальный прогноз")
                        forecast_table = pd.DataFrame({
                            'Период': [f"+{i} мес" for i in range(1, periods+1)],
                            'Прогноз': forecast['forecast'],
                            'Нижняя граница': forecast['lower_bound'],
                            'Верхняя граница': forecast['upper_bound']
                        })
                        st.dataframe(forecast_table, use_container_width=True)
                    else:
                        st.warning("Недостаточно данных для прогноза (нужно минимум 3 периода)")
            else:
                st.info("💡 Для прогнозирования необходима колонка с датами в данных")
        
        with main_tab5:
            st.markdown('<div class="section-header">💾 Сохранение и экспорт</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💾 Сохранить текущий анализ")
                report_name = st.text_input("Название отчета", placeholder="Мой отчет")
                
                if st.button("Сохранить отчет", use_container_width=True) and 'pivot_result' in st.session_state:
                    config = {
                        'rows': rows if 'rows' in locals() else [],
                        'columns': columns if 'columns' in locals() else [],
                        'values': values if 'values' in locals() else [],
                        'timestamp': datetime.now().isoformat()
                    }
                    report_manager.save_report(report_name or f"Отчет_{datetime.now().strftime('%Y%m%d_%H%M%S')}", 
                                              config, st.session_state['pivot_result'])
                    st.success("✅ Отчет успешно сохранен!")
            
            with col2:
                st.markdown("### 📥 Экспорт данных")
                
                export_format = st.selectbox("Формат экспорта", ["CSV", "Excel", "JSON", "HTML"])
                
                if 'pivot_result' in st.session_state:
                    result = st.session_state['pivot_result']
                    
                    if export_format == "CSV":
                        csv = result.to_csv()
                        st.download_button("📥 Скачать CSV", csv, f"report_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv", use_container_width=True)
                    
                    elif export_format == "Excel":
                        output = pd.ExcelWriter('temp_report.xlsx')
                        result.to_excel(output, sheet_name='Pivot Report')
                        output.close()
                        with open('temp_report.xlsx', 'rb') as f:
                            st.download_button("📥 Скачать Excel", f, f"report_{datetime.now().strftime('%Y%m%d')}.xlsx", 
                                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                    
                    elif export_format == "JSON":
                        json_data = result.to_json(orient='records', indent=2)
                        st.download_button("📥 Скачать JSON", json_data, f"report_{datetime.now().strftime('%Y%m%d')}.json", "application/json", use_container_width=True)
                    
                    elif export_format == "HTML":
                        html = result.to_html(classes='table table-striped')
                        st.download_button("📥 Скачать HTML", html, f"report_{datetime.now().strftime('%Y%m%d')}.html", "text/html", use_container_width=True)
            
            # История загрузок
            st.markdown("### 📜 История загрузок")
            history = data_manager.conn.execute("SELECT * FROM load_history ORDER BY load_date DESC LIMIT 10").fetchdf()
            if not history.empty:
                st.dataframe(history[['load_date', 'file_name', 'rows_loaded', 'status']], use_container_width=True)
    
    else:
        # Пустое состояние
        st.markdown("""
        <div style="text-align: center; padding: 50px;">
            <h1>🎯 Добро пожаловать в Ultimate Pivot Analytics!</h1>
            <p style="font-size: 1.2em;">Профессиональная аналитическая платформа нового поколения</p>
            <br>
            <div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">
                <div class="analytics-card" style="width: 250px;">
                    <h3>📁 1. Загрузите данные</h3>
                    <p>Excel, CSV файлы с автоматическим определением структуры</p>
                </div>
                <div class="analytics-card" style="width: 250px;">
                    <h3>🎯 2. Настройте анализ</h3>
                    <p>Строки, колонки, значения - как в Excel, но мощнее</p>
                </div>
                <div class="analytics-card" style="width: 250px;">
                    <h3>📊 3. Анализируйте</h3>
                    <p>Сводные таблицы, прогнозы, корреляции, аномалии</p>
                </div>
            </div>
            <br>
            <p style="color: #667eea;">🚀 Готовы начать? Загрузите файлы через боковую панель!</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Пример данных
        with st.expander("📖 Пример структуры данных"):
            st.code("""
            | Дата       | Продукт  | Категория  | Продажи | Количество |
            |------------|----------|------------|---------|-------------|
            | 2024-01-01 | Ноутбук  | Электроника| 50000   | 10          |
            | 2024-01-02 | Мышь     | Аксессуары | 1500    | 30          |
            | 2024-01-03 | Клавиатура| Аксессуары| 3000    | 15          |
            
            Автоматически определит:
            - 📐 Измерения: Продукт, Категория
            - 📅 Даты: Дата
            - 💹 Метрики: Продажи, Количество
            """, language="markdown")

if __name__ == "__main__":
    main()
