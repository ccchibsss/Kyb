import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
import glob
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
    /* Главный фон */
    .stApp {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    }
    
    /* Карточки */
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
    
    /* Заголовки секций */
    .section-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        margin: 20px 0;
        font-size: 1.2em;
        font-weight: bold;
    }
    
    /* Метрики */
    .metric-box {
        background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        color: white;
    }
    
    /* Кнопки */
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
    
    /* Таблицы */
    .dataframe {
        border-radius: 10px;
        overflow: hidden;
    }
    
    /* Вкладки */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 10px 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    /* Боковая панель */
    .css-1d391kg {
        background: linear-gradient(180deg, #2c3e50 0%, #3498db 100%);
    }
    
    /* Прогресс бар */
    .stProgress > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (ЗАМЕНА SCIPY)
# ============================================
def calculate_z_scores(data):
    """Расчет Z-score без scipy"""
    mean = np.mean(data)
    std = np.std(data)
    if std == 0:
        return np.zeros_like(data)
    return (data - mean) / std

def calculate_pearson_correlation(x, y):
    """Расчет корреляции Пирсона без scipy"""
    x = np.array(x)
    y = np.array(y)
    
    # Удаляем NaN
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
    # Приблизительный расчет p-value (для демонстрации)
    p_value = 2 * (1 - min(0.9999, abs(t) / (abs(t) + 10)))
    return p_value

# ============================================
# 3. УПРАВЛЕНИЕ ДАННЫМИ
# ============================================
class DataManager:
    def __init__(self):
        self.conn = self.get_connection()
        self.init_database()
    
    @st.cache_resource
    def get_connection(self):
        """Создает подключение к DuckDB"""
        return duckdb.connect('analytics_cube.duckdb')
    
    def init_database(self):
        """Инициализирует базу данных и все таблицы"""
        # Таблица для данных
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_data (
                id INTEGER PRIMARY KEY,
                load_date TIMESTAMP,
                source_file VARCHAR,
                data_hash VARCHAR,
                row_count INTEGER
            )
        """)
        
        # Таблица для сохраненных отчетов
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS saved_reports (
                id INTEGER PRIMARY KEY,
                report_name VARCHAR,
                report_date TIMESTAMP,
                config JSON,
                data_hash VARCHAR,
                is_favorite BOOLEAN
            )
        """)
        
        # Таблица для истории загрузок
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
        
        # Таблица для метаданных колонок
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS column_metadata (
                column_name VARCHAR,
                data_type VARCHAR,
                category VARCHAR,
                last_updated TIMESTAMP,
                unique_values INTEGER,
                null_percentage FLOAT
            )
        """)
    
    def load_excel_files(self, files, table_name='main_data'):
        """Загружает Excel файлы с прогрессом и метаданными"""
        if not files:
            return 0, "Нет файлов"
        
        total_rows = 0
        first_file = True
        start_time = datetime.now()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, file in enumerate(files):
            status_text.text(f"Загрузка {file.name}... ({idx+1}/{len(files)})")
            
            try:
                df = pd.read_excel(file)
                
                # Оптимизация типов
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Пробуем конвертировать в дату
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except:
                            pass
                        
                        # Пробуем конвертировать в число
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
                
                # Сохраняем историю
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'SUCCESS', ?)
                """, [file.name, len(df), 0])
                
                # Обновляем метаданные колонок
                self.update_column_metadata(df)
                
            except Exception as e:
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'ERROR', ?)
                """, [file.name, 0, 0])
                st.error(f"Ошибка загрузки {file.name}: {str(e)}")
            
            progress_bar.progress((idx + 1) / len(files))
        
        duration = (datetime.now() - start_time).total_seconds()
        status_text.text(f"✅ Загружено {total_rows:,} строк за {duration:.1f} сек")
        progress_bar.empty()
        
        return total_rows, f"✅ Загружено {total_rows:,} строк из {len(files)} файлов"
    
    def update_column_metadata(self, df):
        """Обновляет метаданные колонок"""
        for col in df.columns:
            # Определяем категорию колонки
            if pd.api.types.is_numeric_dtype(df[col]):
                category = 'metric'
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                category = 'date'
            else:
                category = 'dimension'
            
            unique_vals = df[col].nunique()
            null_pct = (df[col].isnull().sum() / len(df)) * 100
            
            self.conn.execute("""
                INSERT OR REPLACE INTO column_metadata 
                (column_name, data_type, category, last_updated, unique_values, null_percentage)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            """, [col, str(df[col].dtype), category, unique_vals, null_pct])
    
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
            # Проверяем существование таблицы
            tables = self.conn.execute("SHOW TABLES").fetchdf()
            if table_name not in tables['name'].values:
                return None
            
            # Основная информация
            total_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            # Получаем sample для анализа
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
                    col_info['range'] = {
                        'min': df_sample[col].min(),
                        'max': df_sample[col].max()
                    }
                    info['dates'].append(col_info)
                else:
                    col_info['category'] = 'dimension'
                    # Топ значений
                    top_values = df_sample[col].value_counts().head(10)
                    col_info['top_values'] = top_values.to_dict()
                    info['dimensions'].append(col_info)
            
            return info
        except Exception as e:
            st.error(f"Ошибка получения информации: {str(e)}")
            return None

# ============================================
# 4. РАСШИРЕННАЯ АНАЛИТИКА (БЕЗ SCIPY)
# ============================================
class AdvancedAnalytics:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def time_series_analysis(self, date_column, metric_column, freq='M'):
        """Временной ряд анализ с трендами и сезонностью"""
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
            # Добавляем скользящее среднее
            df['moving_avg_3'] = df['total'].rolling(window=3, min_periods=1).mean()
            df['moving_avg_7'] = df['total'].rolling(window=7, min_periods=1).mean()
            
            # Рост/падение
            df['growth'] = df['total'].pct_change() * 100
            
            # Тренд (линейная регрессия)
            if len(df) > 2:
                x = np.arange(len(df))
                z = np.polyfit(x, df['total'].fillna(0), 1)
                df['trend'] = np.polyval(z, x)
        
        return df
    
    def correlation_analysis(self, metrics):
        """Корреляционный анализ между метриками"""
        if len(metrics) < 2:
            return None
        
        # Получаем данные
        query = f"SELECT {', '.join(metrics)} FROM {self.table_name}"
        df = self.conn.execute(query).fetchdf()
        
        # Корреляционная матрица
        corr_matrix = pd.DataFrame(index=metrics, columns=metrics)
        p_values = pd.DataFrame(index=metrics, columns=metrics)
        
        for i, m1 in enumerate(metrics):
            for j, m2 in enumerate(metrics):
                if i == j:
                    corr_matrix.loc[m1, m2] = 1
                    p_values.loc[m1, m2] = 0
                else:
                    # Используем нашу функцию корреляции
                    r = calculate_pearson_correlation(df[m1].values, df[m2].values)
                    corr_matrix.loc[m1, m2] = r
                    # Расчет p-value
                    n = len(df[m1].dropna())
                    p_values.loc[m1, m2] = calculate_p_value(r, n)
        
        # Конвертируем в float
        corr_matrix = corr_matrix.astype(float)
        p_values = p_values.astype(float)
        
        return {
            'correlation': corr_matrix,
            'p_values': p_values,
            'significant': []
        }
    
    def predictive_forecast(self, date_column, metric_column, periods=12):
        """Простой прогноз на основе линейной регрессии"""
        # Получаем исторические данные
        query = f"""
            SELECT 
                DATE_TRUNC('month', {date_column}) as period,
                SUM({metric_column}) as value
            FROM {self.table_name}
            GROUP BY period
            ORDER BY period
        """
        
        df = self.conn.execute(query).fetchdf()
        
        if len(df) < 3:
            return None
        
        # Создаем прогноз
        x = np.arange(len(df))
        y = df['value'].values
        
        # Линейная регрессия
        z = np.polyfit(x, y, 1)
        p = np.poly1d(z)
        
        # Прогноз на будущие периоды
        future_x = np.arange(len(df), len(df) + periods)
        forecast = p(future_x)
        
        # Доверительные интервалы (упрощенно)
        residuals = y - p(x)
        std_residuals = np.std(residuals)
        upper_bound = forecast + 1.96 * std_residuals
        lower_bound = forecast - 1.96 * std_residuals
        
        return {
            'historical': df,
            'forecast': forecast,
            'upper_bound': upper_bound,
            'lower_bound': lower_bound,
            'periods': future_x
        }
    
    def anomaly_detection(self, metric_column, threshold=3):
        """Обнаружение аномалий с использованием Z-score"""
        query = f"SELECT {metric_column} FROM {self.table_name}"
        values = self.conn.execute(query).fetchdf()[metric_column].values
        
        # Z-score (используем нашу функцию)
        z_scores = calculate_z_scores(values)
        anomalies = np.where(np.abs(z_scores) > threshold)[0]
        
        # IQR метод
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        iqr_anomalies = np.where((values < (q1 - 1.5 * iqr)) | (values > (q3 + 1.5 * iqr)))[0]
        
        return {
            'z_score_anomalies': anomalies,
            'iqr_anomalies': iqr_anomalies,
            'z_scores': z_scores,
            'threshold': threshold
        }
    
    def cohort_analysis(self, dimension, metric, cohort_period='month'):
        """Когортный анализ"""
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
    
    def segmentation(self, segment_column, metrics):
        """Сегментационный анализ"""
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
            """
            stats = self.conn.execute(query).fetchdf()
            segment_stats.append(stats)
        
        return segment_stats

# ============================================
# 5. УМНАЯ СВОДНАЯ ТАБЛИЦА
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
            # Автоматически выбираем первую метрику
            info = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 1").fetchdf()
            metrics = [col for col in info.columns if pd.api.types.is_numeric_dtype(info[col])]
            if metrics:
                values = [metrics[0]]
            else:
                return None, "Нет числовых полей для анализа"
        
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
                """
                
                result = self.conn.execute(query).fetchdf()
                
                # Создаем pivot
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
                    
                    # Проценты от общего
                    if show_percentages and len(pivot_df) > 0:
                        pivot_df = (pivot_df / pivot_df.sum().sum()) * 100
                    
                    # Ранжирование
                    if rank_values and len(pivot_df) > 0:
                        for col in pivot_df.columns:
                            pivot_df[f"{col}_rank"] = pivot_df[col].rank(ascending=False)
                    
                    # Итоги
                    if show_totals:
                        pivot_df['Итого по строкам'] = pivot_df.sum(axis=1)
                        grand_total = pivot_df.sum(axis=0)
                        grand_total.name = 'ВСЕГО'
                        pivot_df = pd.concat([pivot_df, grand_total.to_frame().T])
                    
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
        """Создает аналитическую панель с множеством показателей"""
        
        pivot_df, message = self.create_pivot(rows, columns, values, agg_func, 
                                              show_totals=True, show_percentages=True)
        
        if pivot_df is None:
            return None, message
        
        # Дополнительные метрики
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
# 6. УПРАВЛЕНИЕ ОТЧЕТАМИ
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
# 7. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
def main():
    # Инициализация менеджеров
    data_manager = DataManager()
    analytics = AdvancedAnalytics(data_manager.conn)
    pivot_tool = SmartPivotTable(data_manager.conn)
    report_manager = ReportManager(data_manager.conn)
    
    # Заголовок
    st.title("🚀 Ultimate Pivot Analytics Platform")
    st.markdown("*Профессиональная аналитика без дополнительных зависимостей*")
    
    # ============================================
    # САЙДБАР
    # ============================================
    with st.sidebar:
        st.markdown("## 📊 Управление")
        
        # Вкладки в сайдбаре
        sidebar_tab = st.radio(
            "Меню",
            ["📁 Данные", "💾 Отчеты", "⚙️ Настройки"],
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
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Строк", f"{data_info['total_rows']:,}")
                    st.metric("Колонок", data_info['total_columns'])
                with col2:
                    st.metric("Измерений", len(data_info['dimensions']))
                    st.metric("Метрик", len(data_info['metrics']))
                
                with st.expander("📋 Детали колонок"):
                    for col in data_info['dimensions'][:5]:
                        st.markdown(f"**{col['name']}** (📐) - {col['unique_count']} уникальных")
                    for col in data_info['metrics'][:5]:
                        stats = col.get('stats', {})
                        mean_val = stats.get('mean', 'N/A')
                        if mean_val != 'N/A':
                            st.markdown(f"**{col['name']}** (💹) - {mean_val:.2f}")
                        else:
                            st.markdown(f"**{col['name']}** (💹) - N/A")
        
        elif sidebar_tab == "💾 Отчеты":
            st.markdown("### Сохраненные отчеты")
            
            reports = report_manager.load_reports()
            if not reports.empty:
                for _, report in reports.iterrows():
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{report['report_name']}**")
                            st.caption(report['report_date'].strftime("%Y-%m-%d %H:%M"))
                        with col2:
                            if st.button("⭐" if report['is_favorite'] else "☆", key=f"fav_{report['id']}"):
                                report_manager.toggle_favorite(report['id'], not report['is_favorite'])
                                st.rerun()
            else:
                st.info("Нет сохраненных отчетов")
        
        else:  # Настройки
            st.markdown("### ⚙️ Настройки")
            
            theme = st.selectbox("Тема", ["Светлая", "Темная", "Системная"])
            language = st.selectbox("Язык", ["Русский", "English"])
            
            st.divider()
            
            if st.button("🗑️ Очистить все данные", use_container_width=True):
                try:
                    data_manager.conn.execute("DROP TABLE IF EXISTS main_data")
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
            "🔮 Прогнозы",
            "💾 Сохранить"
        ])
        
        with main_tab1:
            st.markdown('<div class="section-header">📊 Конструктор сводной таблицы</div>', unsafe_allow_html=True)
            
            # Три колонки для настроек
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 📊 Строки")
                dimension_names = [d['name'] for d in data_info['dimensions']] + [d['name'] for d in data_info['dates']]
                rows = st.multiselect("Поля для строк", dimension_names, key="rows")
            
            with col2:
                st.markdown("### 📈 Колонки")
                columns = st.multiselect("Поля для колонок", dimension_names, key="columns")
            
            with col3:
                st.markdown("### 🧮 Значения")
                metric_names = [m['name'] for m in data_info['metrics']]
                values = st.multiselect("Числовые поля", metric_names, key="values")
                
                if values:
                    agg_func = st.selectbox("Агрегация", ["SUM", "COUNT", "AVG", "MIN", "MAX", "STD"])
            
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
            if st.button("🔄 Построить сводную таблицу", type="primary", use_container_width=True):
                with st.spinner("Построение..."):
                    result, msg = pivot_tool.create_pivot(
                        rows, columns, values, agg_func if values else 'SUM',
                        show_totals, show_percentages, rank_values
                    )
                    
                    if result is not None:
                        st.success(msg)
                        
                        # Сохраняем результат в session state
                        st.session_state['result'] = result
                        
                        # Показываем результат
                        st.dataframe(result.style.background_gradient(cmap='Blues'), 
                                    use_container_width=True, height=500)
                        
                        # Быстрая статистика
                        st.markdown("### 📊 Краткая статистика")
                        col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
                        with col_stat1:
                            st.metric("Строк", len(result))
                        with col_stat2:
                            st.metric("Колонок", len(result.columns))
                        with col_stat3:
                            total = result.select_dtypes(include=['number']).sum().sum()
                            st.metric("Общая сумма", f"{total:,.0f}")
                        with col_stat4:
                            avg = result.select_dtypes(include=['number']).mean().mean()
                            st.metric("Среднее", f"{avg:,.2f}")
        
        with main_tab2:
            st.markdown('<div class="section-header">📈 Расширенный аналитический модуль</div>', unsafe_allow_html=True)
            
            analytics_type = st.selectbox(
                "Тип анализа",
                ["Корреляционный анализ", "Когортный анализ", "Сегментация", "Обнаружение аномалий", "Временные ряды"]
            )
            
            if analytics_type == "Корреляционный анализ":
                selected_metrics = st.multiselect("Выберите метрики", metric_names, default=metric_names[:min(3, len(metric_names))])
                if len(selected_metrics) >= 2:
                    corr_result = analytics.correlation_analysis(selected_metrics)
                    if corr_result:
                        st.subheader("Корреляционная матрица")
                        fig = px.imshow(
                            corr_result['correlation'],
                            text_auto=True,
                            title="Корреляция между метриками",
                            color_continuous_scale='RdBu',
                            zmin=-1, zmax=1
                        )
                        st.plotly_chart(fig, use_container_width=True)
            
            elif analytics_type == "Обнаружение аномалий":
                metric_for_anomaly = st.selectbox("Выберите метрику", metric_names)
                threshold = st.slider("Порог чувствительности", 1, 5, 3)
                
                if st.button("Найти аномалии"):
                    anomalies = analytics.anomaly_detection(metric_for_anomaly, threshold)
                    
                    st.success(f"Найдено аномалий: {len(anomalies['z_score_anomalies'])} (Z-score) и {len(anomalies['iqr_anomalies'])} (IQR)")
                    
                    # Визуализация
                    fig = go.Figure()
                    values = analytics.conn.execute(f"SELECT {metric_for_anomaly} FROM main_data").fetchdf()[metric_for_anomaly]
                    fig.add_trace(go.Scatter(y=values, mode='lines+markers', name='Значения'))
                    fig.add_trace(go.Scatter(y=anomalies['z_scores'], mode='lines', name='Z-score', yaxis='y2'))
                    fig.update_layout(
                        title=f"Обнаружение аномалий в {metric_for_anomaly}",
                        yaxis=dict(title=metric_for_anomaly),
                        yaxis2=dict(title="Z-score", overlaying='y', side='right')
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with main_tab3:
            st.markdown('<div class="section-header">🎨 Интерактивные визуализации</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Тип визуализации",
                ["Столбчатая диаграмма", "Линейный график", "Круговая диаграмма", 
                 "Ящик с усами", "Тепловая карта"]
            )
            
            if data_info['dimensions']:
                x_axis = st.selectbox("Ось X", [d['name'] for d in data_info['dimensions']])
            else:
                x_axis = None
            
            y_axis = st.selectbox("Ось Y", metric_names)
            
            if viz_type == "Столбчатая диаграмма":
                query = f"SELECT {x_axis}, SUM({y_axis}) as total FROM main_data GROUP BY {x_axis} ORDER BY total DESC LIMIT 20"
                plot_data = data_manager.conn.execute(query).fetchdf()
                
                fig = px.bar(plot_data, x=x_axis, y='total', 
                            title=f"{y_axis} по {x_axis}",
                            color_discrete_sequence=px.colors.qualitative.Set3,
                            text='total')
                fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                st.plotly_chart(fig, use_container_width=True)
            
            elif viz_type == "Тепловая карта":
                # Создаем матрицу корреляции
                corr_data = analytics.correlation_analysis(metric_names[:min(5, len(metric_names))])
                if corr_data:
                    fig = px.imshow(corr_data['correlation'], text_auto=True, 
                                  color_continuous_scale='Viridis',
                                  title="Тепловая карта корреляций")
                    st.plotly_chart(fig, use_container_width=True)
        
        with main_tab4:
            st.markdown('<div class="section-header">🔮 Прогнозирование и тренды</div>', unsafe_allow_html=True)
            
            if data_info['dates']:
                date_col = st.selectbox("Дата", [d['name'] for d in data_info['dates']])
                metric_for_forecast = st.selectbox("Метрика для прогноза", metric_names)
                periods = st.slider("Периодов прогноза", 3, 24, 12)
                
                if st.button("Построить прогноз"):
                    forecast = analytics.predictive_forecast(date_col, metric_for_forecast, periods)
                    
                    if forecast:
                        # Исторические данные
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=forecast['historical']['period'],
                            y=forecast['historical']['value'],
                            mode='lines+markers',
                            name='Исторические данные',
                            line=dict(color='blue', width=2)
                        ))
                        
                        # Прогноз
                        future_dates = [forecast['historical']['period'].iloc[-1] + timedelta(days=30*i) 
                                      for i in range(1, periods+1)]
                        fig.add_trace(go.Scatter(
                            x=future_dates,
                            y=forecast['forecast'],
                            mode='lines+markers',
                            name='Прогноз',
                            line=dict(color='red', width=2, dash='dash')
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
                            title=f"Прогноз {metric_for_forecast} на {periods} периодов",
                            xaxis_title="Период",
                            yaxis_title=metric_for_forecast,
                            hovermode='x unified'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Показатели точности
                        st.subheader("Метрики качества прогноза")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Средняя ошибка", f"{np.mean(np.abs(forecast['historical']['value'].diff().dropna())):.2f}")
                        with col2:
                            st.metric("Тренд", f"{forecast['forecast'][-1] - forecast['historical']['value'].iloc[-1]:.2f}")
                        with col3:
                            st.metric("Волатильность", f"{forecast['historical']['value'].std():.2f}")
            else:
                st.info("Для прогнозирования необходимы колонки с датами")
        
        with main_tab5:
            st.markdown('<div class="section-header">💾 Сохранение и экспорт</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 💾 Сохранить отчет")
                report_name = st.text_input("Название отчета")
                if st.button("Сохранить текущий анализ") and 'result' in st.session_state:
                    config = {
                        'rows': rows if 'rows' in locals() else [],
                        'columns': columns if 'columns' in locals() else [],
                        'values': values if 'values' in locals() else [],
                        'timestamp': datetime.now().isoformat()
                    }
                    report_manager.save_report(report_name, config, st.session_state['result'])
                    st.success("Отчет сохранен!")
            
            with col2:
                st.markdown("### 📥 Экспорт данных")
                export_format = st.selectbox("Формат", ["CSV", "Excel", "JSON"])
                
                if 'result' in st.session_state:
                    result = st.session_state['result']
                    if export_format == "CSV":
                        csv = result.to_csv()
                        st.download_button("Скачать CSV", csv, "report.csv", "text/csv")
                    elif export_format == "Excel":
                        output = pd.ExcelWriter('report.xlsx')
                        result.to_excel(output, sheet_name='Pivot Report')
                        output.close()
                        with open('report.xlsx', 'rb') as f:
                            st.download_button("Скачать Excel", f, "report.xlsx")
                    elif export_format == "JSON":
                        json_data = result.to_json(orient='records', indent=2)
                        st.download_button("Скачать JSON", json_data, "report.json")
    
    else:
        # Пустое состояние
        st.markdown("""
        <div style="text-align: center; padding: 50px;">
            <h1>🎯 Добро пожаловать в Ultimate Pivot Analytics!</h1>
            <p style="font-size: 1.2em;">Профессиональная аналитическая платформа нового поколения</p>
            <br>
            <div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">
                <div style="background: white; padding: 20px; border-radius: 10px; width: 250px;">
                    <h3>📁 1. Загрузите данные</h3>
                    <p>Excel, CSV файлы с автоматическим определением структуры</p>
                </div>
                <div style="background: white; padding: 20px; border-radius: 10px; width: 250px;">
                    <h3>🎯 2. Настройте анализ</h3>
                    <p>Строки, колонки, значения - как в Excel, но мощнее</p>
                </div>
                <div style="background: white; padding: 20px; border-radius: 10px; width: 250px;">
                    <h3>📊 3. Анализируйте</h3>
                    <p>Сводные таблицы, прогнозы, корреляции, аномалии</p>
                </div>
            </div>
            <br>
            <p style="color: #667eea;">🚀 Готовы начать? Загрузите файлы через боковую панель!</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
