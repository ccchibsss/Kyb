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
    
    /* Прогресс бар */
    .stProgress > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 2. ГЛОБАЛЬНАЯ ФУНКЦИЯ ДЛЯ ПОДКЛЮЧЕНИЯ (БЕЗ КЭШИРОВАНИЯ МЕТОДА)
# ============================================
@st.cache_resource
def get_duckdb_connection():
    """Создает подключение к DuckDB (кэшируется глобально)"""
    return duckdb.connect('analytics_cube.duckdb')

# ============================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
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
                    config VARCHAR,
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
        except Exception as e:
            st.error(f"Ошибка инициализации БД: {str(e)}")
    
    def load_excel_files(self, files, table_name='main_data'):
        """Загружает Excel файлы"""
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
                
                # Сохраняем историю
                self.conn.execute("""
                    INSERT INTO load_history (load_date, file_name, rows_loaded, status, duration_seconds)
                    VALUES (CURRENT_TIMESTAMP, ?, ?, 'SUCCESS', ?)
                """, [file.name, len(df), 0])
                
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
            if len(tables) == 0 or table_name not in tables['name'].values:
                return None
            
            # Основная информация
            total_rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            if total_rows == 0:
                return None
            
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
                    info['dimensions'].append(col_info)
            
            return info
        except Exception as e:
            st.error(f"Ошибка получения информации: {str(e)}")
            return None

# ============================================
# 5. РАСШИРЕННАЯ АНАЛИТИКА
# ============================================
class AdvancedAnalytics:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def correlation_analysis(self, metrics):
        """Корреляционный анализ"""
        if len(metrics) < 2:
            return None
        
        try:
            query = f"SELECT {', '.join(metrics)} FROM {self.table_name} LIMIT 100000"
            df = self.conn.execute(query).fetchdf()
            
            # Корреляционная матрица
            corr_matrix = pd.DataFrame(index=metrics, columns=metrics)
            
            for i, m1 in enumerate(metrics):
                for j, m2 in enumerate(metrics):
                    if i == j:
                        corr_matrix.loc[m1, m2] = 1
                    else:
                        r = calculate_pearson_correlation(df[m1].values, df[m2].values)
                        corr_matrix.loc[m1, m2] = r
            
            corr_matrix = corr_matrix.astype(float)
            
            return {'correlation': corr_matrix}
        except Exception as e:
            st.error(f"Ошибка корреляции: {str(e)}")
            return None
    
    def anomaly_detection(self, metric_column, threshold=3):
        """Обнаружение аномалий"""
        try:
            query = f"SELECT {metric_column} FROM {self.table_name}"
            values = self.conn.execute(query).fetchdf()[metric_column].values
            
            z_scores = calculate_z_scores(values)
            anomalies = np.where(np.abs(z_scores) > threshold)[0]
            
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            iqr_anomalies = np.where((values < (q1 - 1.5 * iqr)) | (values > (q3 + 1.5 * iqr)))[0]
            
            return {
                'z_score_anomalies': anomalies,
                'iqr_anomalies': iqr_anomalies,
                'z_scores': z_scores,
                'values': values,
                'threshold': threshold
            }
        except Exception as e:
            st.error(f"Ошибка обнаружения аномалий: {str(e)}")
            return None
    
    def predictive_forecast(self, date_column, metric_column, periods=12):
        """Простой прогноз"""
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
            st.error(f"Ошибка прогноза: {str(e)}")
            return None

# ============================================
# 6. УМНАЯ СВОДНАЯ ТАБЛИЦА
# ============================================
class SmartPivotTable:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def create_pivot(self, rows, columns, values, agg_func='SUM', 
                     show_totals=True, show_percentages=False):
        """Создает сводную таблицу"""
        
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
                    LIMIT 10000
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

# ============================================
# 7. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
def main():
    # Инициализация
    conn = get_duckdb_connection()
    data_manager = DataManager(conn)
    analytics = AdvancedAnalytics(conn)
    pivot_tool = SmartPivotTable(conn)
    
    # Заголовок
    st.title("🚀 Ultimate Pivot Analytics Platform")
    st.markdown("*Профессиональная аналитика без дополнительных зависимостей*")
    
    # ============================================
    # САЙДБАР
    # ============================================
    with st.sidebar:
        st.markdown("## 📊 Управление")
        
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
        main_tab1, main_tab2, main_tab3 = st.tabs([
            "📊 Сводная таблица", 
            "📈 Аналитика", 
            "🎨 Визуализации"
        ])
        
        with main_tab1:
            st.markdown('<div class="section-header">📊 Конструктор сводной таблицы</div>', unsafe_allow_html=True)
            
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
                    agg_func = st.selectbox("Агрегация", ["SUM", "COUNT", "AVG", "MIN", "MAX"])
            
            with st.expander("⚙️ Расширенные настройки"):
                col_opt1, col_opt2 = st.columns(2)
                with col_opt1:
                    show_totals = st.checkbox("Показывать итоги", True)
                with col_opt2:
                    show_percentages = st.checkbox("Показывать проценты", False)
            
            if st.button("🔄 Построить сводную таблицу", type="primary", use_container_width=True):
                with st.spinner("Построение..."):
                    result, msg = pivot_tool.create_pivot(
                        rows, columns, values, agg_func if values else 'SUM',
                        show_totals, show_percentages
                    )
                    
                    if result is not None:
                        st.success(msg)
                        st.session_state['result'] = result
                        
                        st.dataframe(result.style.background_gradient(cmap='Blues'), 
                                    use_container_width=True, height=500)
                        
                        # Статистика
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
                        
                        # Экспорт
                        csv = result.to_csv()
                        st.download_button("📥 Скачать CSV", csv, "pivot_report.csv", "text/csv")
                    else:
                        st.error(msg)
        
        with main_tab2:
            st.markdown('<div class="section-header">📈 Аналитический модуль</div>', unsafe_allow_html=True)
            
            analytics_type = st.selectbox(
                "Тип анализа",
                ["Корреляционный анализ", "Обнаружение аномалий"]
            )
            
            if analytics_type == "Корреляционный анализ":
                metric_names = [m['name'] for m in data_info['metrics']]
                selected_metrics = st.multiselect("Выберите метрики", metric_names, 
                                                  default=metric_names[:min(3, len(metric_names))])
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
                metric_names = [m['name'] for m in data_info['metrics']]
                metric_for_anomaly = st.selectbox("Выберите метрику", metric_names)
                threshold = st.slider("Порог чувствительности", 1, 5, 3)
                
                if st.button("Найти аномалии"):
                    anomalies = analytics.anomaly_detection(metric_for_anomaly, threshold)
                    if anomalies:
                        st.success(f"Найдено аномалий: {len(anomalies['z_score_anomalies'])}")
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            y=anomalies['values'], 
                            mode='lines+markers', 
                            name='Значения',
                            line=dict(color='blue')
                        ))
                        
                        # Отмечаем аномалии
                        if len(anomalies['z_score_anomalies']) > 0:
                            anomaly_values = anomalies['values'][anomalies['z_score_anomalies']]
                            fig.add_trace(go.Scatter(
                                y=anomaly_values,
                                mode='markers',
                                name='Аномалии',
                                marker=dict(color='red', size=10, symbol='x')
                            ))
                        
                        fig.update_layout(
                            title=f"Обнаружение аномалий в {metric_for_anomaly}",
                            yaxis_title=metric_for_anomaly
                        )
                        st.plotly_chart(fig, use_container_width=True)
        
        with main_tab3:
            st.markdown('<div class="section-header">🎨 Визуализации</div>', unsafe_allow_html=True)
            
            viz_type = st.selectbox(
                "Тип визуализации",
                ["Столбчатая диаграмма", "Линейный график", "Круговая диаграмма"]
            )
            
            if data_info['dimensions']:
                x_axis = st.selectbox("Ось X", [d['name'] for d in data_info['dimensions']])
                y_axis = st.selectbox("Ось Y", [m['name'] for m in data_info['metrics']])
                
                if st.button("Построить график"):
                    query = f"SELECT {x_axis}, SUM({y_axis}) as total FROM main_data GROUP BY {x_axis} ORDER BY total DESC LIMIT 20"
                    plot_data = conn.execute(query).fetchdf()
                    
                    if viz_type == "Столбчатая диаграмма":
                        fig = px.bar(plot_data, x=x_axis, y='total', title=f"{y_axis} по {x_axis}")
                        st.plotly_chart(fig, use_container_width=True)
                    elif viz_type == "Линейный график":
                        fig = px.line(plot_data, x=x_axis, y='total', title=f"Тренд {y_axis}")
                        st.plotly_chart(fig, use_container_width=True)
                    elif viz_type == "Круговая диаграмма":
                        fig = px.pie(plot_data.head(10), values='total', names=x_axis, title=f"Распределение {y_axis}")
                        st.plotly_chart(fig, use_container_width=True)
    
    else:
        # Пустое состояние
        st.info("""
        # 🎯 Добро пожаловать в Ultimate Pivot Analytics!
        
        ## Как начать:
        
        1. **Загрузите Excel файл** через боковую панель слева
        2. **Выберите поля** для строк, колонок и значений
        3. **Анализируйте результат** в интерактивных вкладках
        
        💡 *Поддерживаются форматы: XLSX, XLS, CSV*
        """)

if __name__ == "__main__":
    main()
