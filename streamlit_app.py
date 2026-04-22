import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json
import hashlib
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================
# 1. НАСТРОЙКА СТРАНИЦЫ
# ============================================
st.set_page_config(
    page_title="Ultimate Pivot Analytics Pro",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Профессиональный CSS
st.markdown("""
<style>
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 12px 24px;
        font-weight: bold;
        transition: all 0.3s;
        width: 100%;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 5px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: bold;
        transition: all 0.3s;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        padding: 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .info-box {
        background: #f8f9fa;
        border-left: 4px solid #667eea;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# 2. ПОДКЛЮЧЕНИЕ К БД
# ============================================
@st.cache_resource
def get_connection():
    return duckdb.connect('analytics_pro.db')

# ============================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def calculate_correlation(x, y):
    x = np.array(x)
    y = np.array(y)
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]
    if len(x) < 2:
        return 0
    return np.corrcoef(x, y)[0, 1]

def detect_anomalies(data, threshold=3):
    mean = np.mean(data)
    std = np.std(data)
    if std == 0:
        return []
    z_scores = (data - mean) / std
    return np.where(np.abs(z_scores) > threshold)[0]

# ============================================
# 4. КЛАСС УПРАВЛЕНИЯ ДАННЫМИ
# ============================================
class DataManager:
    def __init__(self, conn):
        self.conn = conn
        self._init_tables()
    
    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS saved_reports (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                date TIMESTAMP,
                config VARCHAR,
                is_favorite BOOLEAN
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS load_history (
                id INTEGER PRIMARY KEY,
                load_date TIMESTAMP,
                file_name VARCHAR,
                rows_loaded INTEGER,
                status VARCHAR
            )
        """)
    
    def load_files(self, files, table_name='main_data'):
        if not files:
            return 0
        
        total = 0
        first = True
        
        for file in files:
            df = pd.read_excel(file)
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except:
                            pass
            
            if first:
                self.conn.register('temp', df)
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp")
                first = False
            else:
                self.conn.register('temp', df)
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp")
            
            total += len(df)
            
            self.conn.execute("""
                INSERT INTO load_history (load_date, file_name, rows_loaded, status)
                VALUES (CURRENT_TIMESTAMP, ?, ?, 'SUCCESS')
            """, [file.name, len(df)])
        
        return total
    
    def add_files(self, files, table_name='main_data'):
        if not files:
            return 0
        
        total = 0
        for file in files:
            df = pd.read_excel(file)
            self.conn.register('temp', df)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp")
            total += len(df)
            
            self.conn.execute("""
                INSERT INTO load_history (load_date, file_name, rows_loaded, status)
                VALUES (CURRENT_TIMESTAMP, ?, ?, 'APPEND')
            """, [file.name, len(df)])
        
        return total
    
    def get_info(self, table_name='main_data'):
        try:
            tables = self.conn.execute("SHOW TABLES").fetchdf()
            if len(tables) == 0 or table_name not in tables['name'].values:
                return None
            
            rows = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if rows == 0:
                return None
            
            sample = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1000").fetchdf()
            
            info = {
                'rows': rows,
                'cols': len(sample.columns),
                'dimensions': [],
                'metrics': [],
                'dates': [],
                'all_columns': list(sample.columns)
            }
            
            for col in sample.columns:
                if pd.api.types.is_numeric_dtype(sample[col]):
                    info['metrics'].append(col)
                elif pd.api.types.is_datetime64_any_dtype(sample[col]):
                    info['dates'].append(col)
                else:
                    info['dimensions'].append(col)
            
            return info
        except Exception as e:
            return None
    
    def get_load_history(self):
        try:
            return self.conn.execute("SELECT * FROM load_history ORDER BY load_date DESC LIMIT 20").fetchdf()
        except:
            return pd.DataFrame()
    
    def clear_data(self, table_name='main_data'):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            return True
        except:
            return False

# ============================================
# 5. КЛАСС АНАЛИТИКИ (ПОЛНАЯ ВЕРСИЯ)
# ============================================
class PivotAnalyzer:
    def __init__(self, conn, table_name='main_data'):
        self.conn = conn
        self.table_name = table_name
    
    def create_pivot(self, rows, columns, values, agg='SUM', show_totals=True, show_percents=False, rank_values=False):
        if not values:
            return None, "Выберите значения для анализа"
        
        try:
            group_by = rows + columns
            
            if group_by:
                agg_exprs = [f"{agg}(\"{v}\") as \"{v}\"" for v in values]
                group_cols = [f"\"{c}\"" for c in group_by]
                
                query = f"""
                    SELECT {', '.join(group_cols)}, {', '.join(agg_exprs)}
                    FROM {self.table_name}
                    GROUP BY {', '.join(group_cols)}
                    ORDER BY {', '.join(group_cols)}
                    LIMIT 100000
                """
                result = self.conn.execute(query).fetchdf()
                
                if columns and len(result) > 0:
                    pivot = result.pivot_table(
                        index=rows if rows else None,
                        columns=columns,
                        values=values[0] if len(values) == 1 else values,
                        aggfunc=agg.lower(),
                        fill_value=0
                    )
                    
                    if show_percents and pivot.sum().sum() > 0:
                        pivot = (pivot / pivot.sum().sum()) * 100
                    
                    if rank_values:
                        for col in pivot.columns:
                            pivot[f"{col}_rank"] = pivot[col].rank(ascending=False)
                    
                    if show_totals:
                        pivot.loc['ИТОГО'] = pivot.sum()
                        pivot['ИТОГО'] = pivot.sum(axis=1)
                    
                    return pivot, f"✅ {len(pivot)} строк × {len(pivot.columns)} столбцов"
                else:
                    return result, f"✅ {len(result)} строк"
            else:
                agg_exprs = [f"{agg}(\"{v}\") as \"{v}\"" for v in values]
                query = f"SELECT {', '.join(agg_exprs)} FROM {self.table_name}"
                result = self.conn.execute(query).fetchdf()
                return result, "✅ Агрегированные данные"
        except Exception as e:
            return None, f"❌ {str(e)}"
    
    def correlation_analysis(self, metrics):
        if len(metrics) < 2:
            return None
        
        try:
            query = f"SELECT \"{'\", \"'.join(metrics)}\" FROM {self.table_name} LIMIT 100000"
            df = self.conn.execute(query).fetchdf()
            
            corr = pd.DataFrame(index=metrics, columns=metrics)
            p_values = pd.DataFrame(index=metrics, columns=metrics)
            
            for i, m1 in enumerate(metrics):
                for j, m2 in enumerate(metrics):
                    if i == j:
                        corr.loc[m1, m2] = 1
                        p_values.loc[m1, m2] = 0
                    else:
                        r = calculate_correlation(df[m1], df[m2])
                        corr.loc[m1, m2] = r
            
            return corr.astype(float)
        except Exception as e:
            return None
    
    def time_series(self, date_col, metric_col, freq='month'):
        try:
            query = f"""
                SELECT 
                    DATE_TRUNC('{freq}', "{date_col}") as period,
                    SUM("{metric_col}") as value,
                    AVG("{metric_col}") as avg,
                    COUNT(*) as count
                FROM {self.table_name}
                GROUP BY period
                ORDER BY period
            """
            df = self.conn.execute(query).fetchdf()
            
            if len(df) > 2:
                df['moving_avg_3'] = df['value'].rolling(3, min_periods=1).mean()
                df['moving_avg_7'] = df['value'].rolling(7, min_periods=1).mean()
                df['growth'] = df['value'].pct_change() * 100
                
                x = np.arange(len(df))
                z = np.polyfit(x, df['value'].fillna(0), 1)
                df['trend'] = np.polyval(z, x)
            
            return df
        except Exception as e:
            return None
    
    def forecast(self, date_col, metric_col, periods=12):
        try:
            query = f"""
                SELECT 
                    DATE_TRUNC('month', "{date_col}") as period,
                    SUM("{metric_col}") as value
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
            std_res = np.std(residuals)
            upper = forecast + 1.96 * std_res
            lower = forecast - 1.96 * std_res
            
            return {
                'historical': df,
                'forecast': forecast,
                'upper': upper,
                'lower': lower,
                'periods': periods
            }
        except Exception as e:
            return None
    
    def anomaly_detection(self, metric_col, threshold=3):
        try:
            query = f"SELECT \"{metric_col}\" FROM {self.table_name}"
            values = self.conn.execute(query).fetchdf()[metric_col].values
            
            z_scores = (values - np.mean(values)) / np.std(values)
            anomalies = np.where(np.abs(z_scores) > threshold)[0]
            
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            iqr_anomalies = np.where((values < (q1 - 1.5 * iqr)) | (values > (q3 + 1.5 * iqr)))[0]
            
            return {
                'z_score_anomalies': anomalies,
                'iqr_anomalies': iqr_anomalies,
                'z_scores': z_scores,
                'values': values
            }
        except Exception as e:
            return None
    
    def segmentation(self, segment_col, metric_col):
        try:
            query = f"""
                SELECT 
                    "{segment_col}",
                    COUNT(*) as count,
                    SUM("{metric_col}") as total,
                    AVG("{metric_col}") as avg,
                    MIN("{metric_col}") as min,
                    MAX("{metric_col}") as max,
                    STDDEV("{metric_col}") as std
                FROM {self.table_name}
                GROUP BY "{segment_col}"
                ORDER BY total DESC
                LIMIT 50
            """
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            return None
    
    def cohort_analysis(self, dimension, metric):
        try:
            query = f"""
                SELECT 
                    "{dimension}" as cohort,
                    COUNT(*) as count,
                    SUM("{metric}") as total_value,
                    AVG("{metric}") as avg_value
                FROM {self.table_name}
                GROUP BY "{dimension}"
                ORDER BY total_value DESC
            """
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            return None
    
    def get_top_values(self, dimension, metric, top_n=10):
        try:
            query = f"""
                SELECT 
                    "{dimension}", 
                    SUM("{metric}") as total
                FROM {self.table_name}
                GROUP BY "{dimension}"
                ORDER BY total DESC
                LIMIT {top_n}
            """
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            return None

# ============================================
# 6. КЛАСС УПРАВЛЕНИЯ ОТЧЕТАМИ
# ============================================
class ReportManager:
    def __init__(self, conn):
        self.conn = conn
    
    def save(self, name, config):
        self.conn.execute("""
            INSERT INTO saved_reports (name, date, config, is_favorite)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?)
        """, [name, json.dumps(config), False])
    
    def load_all(self):
        return self.conn.execute("SELECT * FROM saved_reports ORDER BY date DESC").fetchdf()
    
    def load_favorites(self):
        return self.conn.execute("SELECT * FROM saved_reports WHERE is_favorite = TRUE ORDER BY date DESC").fetchdf()
    
    def toggle_favorite(self, report_id):
        current = self.conn.execute("SELECT is_favorite FROM saved_reports WHERE id = ?", [report_id]).fetchone()[0]
        self.conn.execute("UPDATE saved_reports SET is_favorite = ? WHERE id = ?", [not current, report_id])
    
    def delete(self, report_id):
        self.conn.execute("DELETE FROM saved_reports WHERE id = ?", [report_id])

# ============================================
# 7. ОСНОВНОЙ ИНТЕРФЕЙС (ПОЛНАЯ ВЕРСИЯ)
# ============================================
def main():
    conn = get_connection()
    data_mgr = DataManager(conn)
    pivot = PivotAnalyzer(conn)
    report_mgr = ReportManager(conn)
    
    st.title("🚀 Ultimate Pivot Analytics Pro")
    st.markdown("*Полноценная аналитическая платформа: сводные таблицы, корреляции, прогнозы, аномалии, временные ряды*")
    st.markdown("---")
    
    # ============================================
    # САЙДБАР
    # ============================================
    with st.sidebar:
        st.markdown("## 📁 Управление данными")
        
        uploaded_files = st.file_uploader(
            "Загрузите Excel файлы",
            type=['xlsx', 'xls', 'csv'],
            accept_multiple_files=True
        )
        
        if uploaded_files:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🆕 Новая загрузка"):
                    rows = data_mgr.load_files(uploaded_files)
                    st.success(f"✅ Загружено {rows:,} строк")
                    st.rerun()
            with col2:
                if st.button("➕ Добавить"):
                    rows = data_mgr.add_files(uploaded_files)
                    st.success(f"✅ Добавлено {rows:,} строк")
                    st.rerun()
        
        st.markdown("---")
        
        info = data_mgr.get_info()
        if info:
            st.markdown("## 📊 Статистика")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Строк", f"{info['rows']:,}")
                st.metric("Измерений", len(info['dimensions']))
            with col2:
                st.metric("Колонок", info['cols'])
                st.metric("Метрик", len(info['metrics']))
            
            if info['dates']:
                st.info(f"📅 Даты: {', '.join(info['dates'][:2])}")
            
            st.markdown("---")
            
            with st.expander("📜 История загрузок"):
                history = data_mgr.get_load_history()
                if not history.empty:
                    st.dataframe(history[['load_date', 'file_name', 'rows_loaded', 'status']], use_container_width=True)
            
            if st.button("🗑️ Очистить данные"):
                if data_mgr.clear_data():
                    st.success("Данные очищены")
                    st.rerun()
        else:
            st.info("ℹ️ Нет данных\nЗагрузите Excel файл")
    
    # ============================================
    # ОСНОВНАЯ ОБЛАСТЬ - 6 ВКЛАДОК
    # ============================================
    
    info = data_mgr.get_info()
    
    if info and info['rows'] > 0:
        st.success(f"✅ Данные загружены: {info['rows']:,} строк, {info['cols']} колонок")
        
        tabs = st.tabs([
            "📊 СВОДНАЯ ТАБЛИЦА",
            "📈 КОРРЕЛЯЦИЯ",
            "📉 ВРЕМЕННЫЕ РЯДЫ",
            "🔮 ПРОГНОЗЫ",
            "⚠️ АНОМАЛИИ",
            "💾 СОХРАНЕНИЕ"
        ])
        
        # ============================================
        # ВКЛАДКА 1 - СВОДНАЯ ТАБЛИЦА
        # ============================================
        with tabs[0]:
            st.markdown("### 📊 Конструктор сводной таблицы")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**📌 СТРОКИ**")
                rows = st.multiselect("", info['dimensions'] + info['dates'], key="rows")
            
            with col2:
                st.markdown("**📌 КОЛОНКИ**")
                cols = st.multiselect("", info['dimensions'] + info['dates'], key="cols")
            
            with col3:
                st.markdown("**📌 ЗНАЧЕНИЯ**")
                values = st.multiselect("", info['metrics'], key="values")
                if values:
                    agg = st.selectbox("Агрегация", ["SUM", "COUNT", "AVG", "MIN", "MAX", "STD"])
            
            with st.expander("⚙️ Дополнительные настройки"):
                col_opt1, col_opt2, col_opt3 = st.columns(3)
                with col_opt1:
                    show_totals = st.checkbox("Показывать итоги", True)
                with col_opt2:
                    show_percents = st.checkbox("Показывать проценты", False)
                with col_opt3:
                    rank_values = st.checkbox("Ранжировать значения", False)
            
            if st.button("🔄 ПОСТРОИТЬ СВОДНУЮ ТАБЛИЦУ", type="primary"):
                if values:
                    with st.spinner("Построение..."):
                        result, msg = pivot.create_pivot(rows, cols, values, agg if values else 'SUM', 
                                                        show_totals, show_percents, rank_values)
                        
                        if result is not None:
                            st.success(msg)
                            st.dataframe(result.style.background_gradient(cmap='Blues'), 
                                        use_container_width=True, height=500)
                            
                            st.session_state['current_pivot'] = result
                            
                            # Статистика
                            st.markdown("### 📊 Статистика")
                            a, b, c, d = st.columns(4)
                            with a:
                                st.metric("Строк", len(result))
                            with b:
                                st.metric("Колонок", len(result.columns))
                            with c:
                                total = result.select_dtypes(include=['number']).sum().sum()
                                st.metric("Сумма", f"{total:,.0f}")
                            with d:
                                avg = result.select_dtypes(include=['number']).mean().mean()
                                st.metric("Среднее", f"{avg:,.2f}")
                        else:
                            st.error(msg)
                else:
                    st.warning("Выберите значения для анализа")
        
        # ============================================
        # ВКЛАДКА 2 - КОРРЕЛЯЦИЯ
        # ============================================
        with tabs[1]:
            st.markdown("### 📈 Корреляционный анализ")
            
            if len(info['metrics']) >= 2:
                selected_metrics = st.multiselect(
                    "Выберите метрики для анализа",
                    info['metrics'],
                    default=info['metrics'][:min(4, len(info['metrics']))]
                )
                
                if len(selected_metrics) >= 2 and st.button("Построить корреляцию"):
                    corr_matrix = pivot.correlation_analysis(selected_metrics)
                    
                    if corr_matrix is not None:
                        fig = px.imshow(
                            corr_matrix,
                            text_auto=True,
                            title="Корреляционная матрица",
                            color_continuous_scale='RdBu',
                            zmin=-1, zmax=1,
                            aspect='auto',
                            width=600,
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("### 🔗 Сильные корреляции")
                        strong = []
                        for i in range(len(selected_metrics)):
                            for j in range(i+1, len(selected_metrics)):
                                r = corr_matrix.iloc[i, j]
                                if abs(r) > 0.5:
                                    strong.append({
                                        'Метрика 1': selected_metrics[i],
                                        'Метрика 2': selected_metrics[j],
                                        'Корреляция': f"{r:.3f}",
                                        'Сила': 'Сильная положительная' if r > 0 else 'Сильная отрицательная'
                                    })
                        
                        if strong:
                            st.dataframe(pd.DataFrame(strong), use_container_width=True)
                        else:
                            st.info("Нет сильных корреляций (|r| > 0.5)")
            else:
                st.warning("Нужно минимум 2 метрики для корреляционного анализа")
        
        # ============================================
        # ВКЛАДКА 3 - ВРЕМЕННЫЕ РЯДЫ
        # ============================================
        with tabs[2]:
            st.markdown("### 📉 Анализ временных рядов")
            
            if info['dates']:
                col1, col2, col3 = st.columns(3)
                with col1:
                    date_col = st.selectbox("Колонка с датой", info['dates'])
                with col2:
                    metric_col = st.selectbox("Метрика", info['metrics'])
                with col3:
                    freq = st.selectbox("Период", ["День", "Неделя", "Месяц", "Квартал"])
                    freq_map = {"День": "day", "Неделя": "week", "Месяц": "month", "Квартал": "quarter"}
                
                if st.button("Построить временной ряд"):
                    ts_data = pivot.time_series(date_col, metric_col, freq_map[freq])
                    
                    if ts_data is not None and len(ts_data) > 0:
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=ts_data['period'],
                            y=ts_data['value'],
                            mode='lines+markers',
                            name='Факт',
                            line=dict(color='blue', width=2),
                            marker=dict(size=6)
                        ))
                        
                        if 'moving_avg_3' in ts_data.columns:
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['moving_avg_3'],
                                mode='lines',
                                name='MA(3)',
                                line=dict(color='orange', width=2, dash='dash')
                            ))
                        
                        if 'moving_avg_7' in ts_data.columns:
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['moving_avg_7'],
                                mode='lines',
                                name='MA(7)',
                                line=dict(color='green', width=2, dash='dot')
                            ))
                        
                        if 'trend' in ts_data.columns:
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['trend'],
                                mode='lines',
                                name='Тренд',
                                line=dict(color='red', width=2, dash='dash')
                            ))
                        
                        fig.update_layout(
                            title=f"Временной ряд: {metric_col}",
                            xaxis_title="Период",
                            yaxis_title=metric_col,
                            hovermode='x unified',
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        if 'growth' in ts_data.columns:
                            st.subheader("📈 Динамика роста")
                            growth_data = ts_data[['period', 'growth']].dropna()
                            fig_growth = px.bar(
                                growth_data,
                                x='period',
                                y='growth',
                                title="Изменение в %",
                                color='growth',
                                color_continuous_scale='RdYlGn'
                            )
                            st.plotly_chart(fig_growth, use_container_width=True)
            else:
                st.warning("Нет колонок с датами для временного анализа")
        
        # ============================================
        # ВКЛАДКА 4 - ПРОГНОЗЫ
        # ============================================
        with tabs[3]:
            st.markdown("### 🔮 Прогнозирование")
            
            if info['dates']:
                col1, col2, col3 = st.columns(3)
                with col1:
                    date_col = st.selectbox("Дата", info['dates'], key="forecast_date")
                with col2:
                    metric_col = st.selectbox("Метрика", info['metrics'], key="forecast_metric")
                with col3:
                    periods = st.slider("Периодов прогноза", 3, 24, 12)
                
                if st.button("🔮 ПОСТРОИТЬ ПРОГНОЗ", type="primary"):
                    forecast_data = pivot.forecast(date_col, metric_col, periods)
                    
                    if forecast_data:
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=forecast_data['historical']['period'],
                            y=forecast_data['historical']['value'],
                            mode='lines+markers',
                            name='История',
                            line=dict(color='blue', width=2),
                            marker=dict(size=6)
                        ))
                        
                        last_date = forecast_data['historical']['period'].iloc[-1]
                        future_dates = [last_date + timedelta(days=30*i) for i in range(1, periods+1)]
                        
                        fig.add_trace(go.Scatter(
                            x=future_dates,
                            y=forecast_data['forecast'],
                            mode='lines+markers',
                            name='Прогноз',
                            line=dict(color='red', width=2, dash='dash'),
                            marker=dict(size=6, symbol='diamond')
                        ))
                        
                        fig.add_trace(go.Scatter(
                            x=future_dates + future_dates[::-1],
                            y=list(forecast_data['upper']) + list(forecast_data['lower'][::-1]),
                            fill='toself',
                            fillcolor='rgba(255,0,0,0.2)',
                            line=dict(color='rgba(255,255,255,0)'),
                            name='95% интервал'
                        ))
                        
                        fig.update_layout(
                            title=f"Прогноз {metric_col} на {periods} периодов",
                            xaxis_title="Период",
                            yaxis_title=metric_col,
                            hovermode='x unified',
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.subheader("📊 Метрики прогноза")
                        col1, col2, col3, col4 = st.columns(4)
                        hist_values = forecast_data['historical']['value']
                        with col1:
                            st.metric("Среднее", f"{hist_values.mean():.2f}")
                        with col2:
                            st.metric("Рост", f"{forecast_data['forecast'][-1] - hist_values.iloc[-1]:.2f}")
                        with col3:
                            st.metric("Волатильность", f"{hist_values.std():.2f}")
                        with col4:
                            growth = ((forecast_data['forecast'][-1] - hist_values.iloc[-1]) / hist_values.iloc[-1]) * 100
                            st.metric("Рост %", f"{growth:.1f}%")
                        
                        st.subheader("📋 Детальный прогноз")
                        forecast_table = pd.DataFrame({
                            'Период': [f"+{i}" for i in range(1, periods+1)],
                            'Прогноз': forecast_data['forecast'],
                            'Нижняя граница': forecast_data['lower'],
                            'Верхняя граница': forecast_data['upper']
                        })
                        st.dataframe(forecast_table, use_container_width=True)
                    else:
                        st.warning("Недостаточно данных для прогноза (нужно минимум 3 периода)")
            else:
                st.warning("Нет колонок с датами для прогнозирования")
        
        # ============================================
        # ВКЛАДКА 5 - АНОМАЛИИ И СЕГМЕНТАЦИЯ
        # ============================================
        with tabs[4]:
            st.markdown("### ⚠️ Обнаружение аномалий")
            
            if info['metrics']:
                metric_for_anomaly = st.selectbox("Выберите метрику", info['metrics'], key="anomaly_metric")
                threshold = st.slider("Порог чувствительности", 1, 5, 3, 
                                     help="Больше значение = меньше аномалий")
                
                if st.button("Найти аномалии"):
                    anomalies = pivot.anomaly_detection(metric_for_anomaly, threshold)
                    
                    if anomalies:
                        st.success(f"🔴 Найдено аномалий: {len(anomalies['z_score_anomalies'])} (Z-score) и {len(anomalies['iqr_anomalies'])} (IQR)")
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            y=anomalies['values'],
                            mode='lines+markers',
                            name='Значения',
                            line=dict(color='blue', width=2),
                            marker=dict(size=6)
                        ))
                        
                        if len(anomalies['z_score_anomalies']) > 0:
                            anomaly_values = anomalies['values'][anomalies['z_score_anomalies']]
                            fig.add_trace(go.Scatter(
                                y=anomaly_values,
                                mode='markers',
                                name='Аномалии',
                                marker=dict(color='red', size=12, symbol='x')
                            ))
                        
                        fig.update_layout(
                            title=f"Обнаружение аномалий в {metric_for_anomaly}",
                            xaxis_title="Индекс",
                            yaxis_title=metric_for_anomaly,
                            height=500
                        )
                        st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            st.markdown("### 📊 Сегментационный анализ")
            
            if info['dimensions'] and info['metrics']:
                col1, col2 = st.columns(2)
                with col1:
                    segment_col = st.selectbox("Измерение для сегментации", info['dimensions'])
                with col2:
                    metric_for_seg = st.selectbox("Метрика", info['metrics'], key="seg_metric")
                
                if st.button("Провести сегментацию"):
                    seg_data = pivot.segmentation(segment_col, metric_for_seg)
                    if seg_data is not None:
                        st.dataframe(seg_data, use_container_width=True)
                        
                        fig = px.bar(seg_data.head(15), x=segment_col, y='total',
                                    title=f"Сегментация по {segment_col}",
                                    color='total',
                                    text='total')
                        fig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
                        st.plotly_chart(fig, use_container_width=True)
        
        # ============================================
        # ВКЛАДКА 6 - СОХРАНЕНИЕ
        # ============================================
        with tabs[5]:
            st.markdown("### 💾 Сохранение и экспорт")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 💾 Сохранить отчет")
                report_name = st.text_input("Название отчета", placeholder="Мой отчет")
                
                if st.button("Сохранить текущий отчет") and 'current_pivot' in st.session_state:
                    config = {
                        'rows': rows if 'rows' in locals() else [],
                        'cols': cols if 'cols' in locals() else [],
                        'values': values if 'values' in locals() else [],
                        'agg': agg if 'agg' in locals() else 'SUM',
                        'timestamp': datetime.now().isoformat()
                    }
                    report_mgr.save(report_name or f"Отчет_{datetime.now().strftime('%Y%m%d_%H%M%S')}", config)
                    st.success("✅ Отчет сохранен!")
            
            with col2:
                st.markdown("#### 📥 Экспорт данных")
                
                if 'current_pivot' in st.session_state:
                    result = st.session_state['current_pivot']
                    format_type = st.selectbox("Формат", ["CSV", "Excel", "JSON"])
                    
                    if format_type == "CSV":
                        csv = result.to_csv()
                        st.download_button("📥 Скачать CSV", csv, "pivot_report.csv", "text/csv")
                    elif format_type == "Excel":
                        output = pd.ExcelWriter('temp_pivot.xlsx')
                        result.to_excel(output, sheet_name='Pivot Table')
                        output.close()
                        with open('temp_pivot.xlsx', 'rb') as f:
                            st.download_button("📥 Скачать Excel", f, "pivot_report.xlsx")
                    elif format_type == "JSON":
                        json_data = result.to_json(orient='records', indent=2)
                        st.download_button("📥 Скачать JSON", json_data, "pivot_report.json")
                else:
                    st.info("Сначала постройте сводную таблицу")
            
            st.markdown("---")
            st.markdown("#### 📋 Сохраненные отчеты")
            
            reports = report_mgr.load_all()
            if not reports.empty:
                for _, report in reports.iterrows():
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"**{report['name']}**")
                        st.caption(report['date'].strftime("%Y-%m-%d %H:%M"))
                    with col2:
                        if st.button("⭐" if report['is_favorite'] else "☆", key=f"fav_{report['id']}"):
                            report_mgr.toggle_favorite(report['id'])
                            st.rerun()
                    with col3:
                        if st.button("🗑️", key=f"del_{report['id']}"):
                            report_mgr.delete(report['id'])
                            st.rerun()
            else:
                st.info("Нет сохраненных отчетов")
    
    else:
        # Приветственный экран
        st.markdown("""
        <div style="text-align: center; padding: 50px;">
            <h1>🎯 Добро пожаловать в Ultimate Pivot Analytics Pro!</h1>
            <p style="font-size: 1.2em;">Полноценная аналитическая платформа с функционалом Excel Power Pivot</p>
            <br>
            <div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">
                <div class="info-box" style="width: 250px;">
                    <h3>📁 1. Загрузите данные</h3>
                    <p>Excel, CSV файлы с автоопределением структуры</p>
                </div>
                <div class="info-box" style="width: 250px;">
                    <h3>🎯 2. Настройте анализ</h3>
                    <p>Строки, колонки, значения - как в Excel</p>
                </div>
                <div class="info-box" style="width: 250px;">
                    <h3>📊 3. Анализируйте</h3>
                    <p>Сводные таблицы, корреляции, прогнозы, аномалии</p>
                </div>
            </div>
            <br>
            <p style="color: #667eea;">🚀 Загрузите файл через боковую панель и начните анализ!</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
