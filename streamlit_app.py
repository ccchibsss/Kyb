import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import json
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

# CSS стили для красоты интерфейса
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
    .info-box {
        background: #f8f9fa;
        border-left: 4px solid #667eea;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .metric-card {
        background: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
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
    try:
        x = np.array(x)
        y = np.array(y)
        mask = ~(np.isnan(x) | np.isnan(y))
        x, y = x[mask], y[mask]
        if len(x) < 2:
            return 0
        return np.corrcoef(x, y)[0, 1]
    except:
        return 0

# ============================================
# 4. КЛАСС УПРАВЛЕНИЯ ДАННЫМИ
# ============================================
class DataManager:
    def __init__(self, conn):
        self.conn = conn
        self._init_tables()

    def _init_tables(self):
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS saved_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR,
                    date TIMESTAMP,
                    config VARCHAR,
                    is_favorite BOOLEAN
                )
            """)
        except:
            pass

    def load_files(self, files, table_name='main_data'):
        if not files:
            return 0

        total_rows = 0
        first = True

        for file in files:
            try:
                df = pd.read_excel(file)
                # Обработка возможных типов данных
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except:
                            try:
                                df[col] = pd.to_numeric(df[col])
                            except:
                                pass
                # Создаём или дополняем таблицу
                if first:
                    self.conn.register('temp_df', df)
                    self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
                    first = False
                else:
                    self.conn.register('temp_df', df)
                    self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                total_rows += len(df)
            except Exception as e:
                filename = getattr(file, 'name', str(file))
                st.error(f"Ошибка при загрузке {filename}: {str(e)}")
        return total_rows

    def add_files(self, files, table_name='main_data'):
        if not files:
            return 0
        total_rows = 0
        for file in files:
            try:
                df = pd.read_excel(file)
                self.conn.register('temp_df', df)
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                total_rows += len(df)
            except Exception as e:
                filename = getattr(file, 'name', str(file))
                st.error(f"Ошибка при добавлении {filename}: {str(e)}")
        return total_rows

    def get_info(self, table_name='main_data'):
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            if result is None or result[0] == 0:
                return None
            total_rows = result[0]
            sample_df = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 1000").fetchdf()

            info = {
                'rows': total_rows,
                'cols': len(sample_df.columns),
                'dimensions': [],
                'metrics': [],
                'dates': [],
                'all_columns': list(sample_df.columns)
            }
            for col in sample_df.columns:
                if pd.api.types.is_numeric_dtype(sample_df[col]):
                    info['metrics'].append(col)
                elif pd.api.types.is_datetime64_any_dtype(sample_df[col]):
                    info['dates'].append(col)
                else:
                    info['dimensions'].append(col)
            return info
        except:
            return None

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
            return None, "Выберите значения"
        try:
            group_by = rows + columns
            if group_by:
                group_cols = [f'"{c}"' for c in group_by]
                agg_exprs = [f'{agg}("{v}") as "{v}"' for v in values]
                query = f"""
                    SELECT {', '.join(group_cols)}, {', '.join(agg_exprs)}
                    FROM {self.table_name}
                    GROUP BY {', '.join(group_cols)}
                    ORDER BY {', '.join(group_cols)}
                    LIMIT 50000
                """
                result_df = self.conn.execute(query).fetchdf()

                # Создаем сводную таблицу (pandas)
                if columns and len(result_df) > 0:
                    pivot_table = result_df.pivot_table(
                        index=rows if rows else None,
                        columns=columns,
                        values=values if len(values) > 1 else values[0],
                        aggfunc=agg.lower(),
                        fill_value=0
                    )

                    if show_percents and pivot_table.sum().sum() > 0:
                        pivot_table = (pivot_table / pivot_table.sum().sum()) * 100

                    if rank_values:
                        for col in pivot_table.columns:
                            if pd.api.types.is_numeric_dtype(pivot_table[col]):
                                pivot_table[f"{col}_rank"] = pivot_table[col].rank(ascending=False)

                    if show_totals:
                        pivot_table.loc['ИТОГО по строкам'] = pivot_table.sum()
                        pivot_table['ИТОГО по колонкам'] = pivot_table.sum(axis=1)

                    return pivot_table, f"✅ {len(pivot_table)} строк × {len(pivot_table.columns)} столбцов"
                else:
                    return result_df, f"✅ {len(result_df)} строк"
            else:
                # Без группировки
                agg_exprs = [f'{agg}("{v}") as "{v}"' for v in values]
                query = f"SELECT {', '.join(agg_exprs)} FROM {self.table_name}"
                result_df = self.conn.execute(query).fetchdf()
                return result_df, "✅ Агрегированные данные"
        except Exception as e:
            return None, f"❌ {str(e)}"

    def correlation_analysis(self, metrics):
        if len(metrics) < 2:
            return None
        try:
            cols = [f'"{m}"' for m in metrics]
            query = f"SELECT {', '.join(cols)} FROM {self.table_name} LIMIT 100000"
            df = self.conn.execute(query).fetchdf()

            corr = pd.DataFrame(index=metrics, columns=metrics)
            for i, m1 in enumerate(metrics):
                for j, m2 in enumerate(metrics):
                    if i == j:
                        corr.loc[m1, m2] = 1
                    else:
                        corr.loc[m1, m2] = calculate_correlation(df[m1], df[m2])
            return corr.astype(float)
        except:
            return None

    def time_series(self, date_col, metric_col, freq='month'):
        try:
            query = f"""
                SELECT 
                    DATE_TRUNC('{freq}', "{date_col}") as period,
                    SUM("{metric_col}") as value
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
                z = np.polyfit(x, df['value'], 1)
                df['trend'] = np.polyval(z, x)
            return df
        except:
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
            forecast_vals = p(future_x)
            residuals = y - p(x)
            std_res = np.std(residuals)
            upper = forecast_vals + 1.96 * std_res
            lower = forecast_vals - 1.96 * std_res
            return {
                'historical': df,
                'forecast': forecast_vals,
                'upper': upper,
                'lower': lower
            }
        except:
            return None

    def anomaly_detection(self, metric_col, threshold=3):
        # Удалена (раздел "Аномалии" убран)
        return None

    def segmentation(self, segment_col, metric_col):
        # Удалена (раздел "Сегментация" убран)
        return None

    def get_top_values(self, dimension, metric, top_n=10):
        try:
            query = f"""
                SELECT "{dimension}", SUM("{metric}") as total
                FROM {self.table_name}
                GROUP BY "{dimension}"
                ORDER BY total DESC
                LIMIT {top_n}
            """
            return self.conn.execute(query).fetchdf()
        except:
            return None

# ============================================
# 6. КЛАСС УПРАВЛЕНИЯ ОТЧЕТАМИ
# ============================================
class ReportManager:
    def __init__(self, conn):
        self.conn = conn

    def save(self, name, config):
        try:
            self.conn.execute("""
                INSERT INTO saved_reports (name, date, config, is_favorite)
                VALUES (?, ?, ?, ?)
            """, [name, datetime.now(), json.dumps(config), False])
            return True
        except:
            return False

    def load_all(self):
        try:
            return self.conn.execute("SELECT * FROM saved_reports ORDER BY date DESC").fetchdf()
        except:
            return pd.DataFrame()

    def toggle_favorite(self, report_id):
        try:
            current = self.conn.execute("SELECT is_favorite FROM saved_reports WHERE id = ?", [report_id]).fetchone()[0]
            self.conn.execute("UPDATE saved_reports SET is_favorite = ? WHERE id = ?", [not current, report_id])
        except:
            pass

    def delete(self, report_id):
        try:
            self.conn.execute("DELETE FROM saved_reports WHERE id = ?", [report_id])
        except:
            pass

# ============================================
# 7. ОСНОВНОЙ ИНТЕРФЕЙС (6 вкладок без "Аномалий" и "Сегментации")
# ============================================
def main():
    conn = get_connection()
    data_mgr = DataManager(conn)
    pivot = PivotAnalyzer(conn)
    report_mgr = ReportManager(conn)

    st.title("🚀 Ultimate Pivot Analytics Pro")
    st.markdown("*Полноценная аналитическая платформа: сводные таблицы, корреляции, прогнозы*")
    st.markdown("---")

    # Sidebar для загрузки данных
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
                if st.button("🆕 Новая загрузка", use_container_width=True):
                    with st.spinner("Загрузка..."):
                        rows = data_mgr.load_files(uploaded_files)
                        st.success(f"✅ Загружено {rows:,} строк")
                        st.rerun()
            with col2:
                if st.button("➕ Добавить файлы", use_container_width=True):
                    with st.spinner("Добавление..."):
                        rows = data_mgr.add_files(uploaded_files)
                        st.success(f"✅ Добавлено {rows:,} строк")
                        st.rerun()

        st.markdown("---")
        info = data_mgr.get_info()
        if info:
            st.markdown("## 📊 Статистика")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("📄 Строк", f"{info['rows']:,}")
                st.metric("📐 Измерений", len(info['dimensions']))
            with col2:
                st.metric("📋 Колонок", info['cols'])
                st.metric("💹 Метрик", len(info['metrics']))
            if info['dates']:
                st.info(f"📅 Даты: {', '.join(info['dates'][:2])}")
            st.markdown("---")
            if st.button("🗑️ Очистить данные", use_container_width=True):
                if data_mgr.clear_data():
                    st.success("Данные очищены")
                    st.rerun()
        else:
            st.info("ℹ️ Нет данных\nЗагрузите Excel файл")

    # Основная вкладка с 4 разделами
    info = data_mgr.get_info()
    if info and info['rows'] > 0:
        st.success(f"✅ Данные загружены: {info['rows']:,} строк, {info['cols']} колонок")
        tabs = st.tabs([
            "📊 Сводная таблица",
            "📈 Корреляция",
            "📉 Временные ряды",
            "🔮 Прогнозы"
        ])

        # Вкладка 1 - Сводная таблица
        with tabs[0]:
            st.markdown("### 📊 Конструктор сводной таблицы")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown("**📌 Строки**")
                rows = st.multiselect("", info['dimensions'] + info['dates'], key="rows")
            with col2:
                st.markdown("**📌 Колонки**")
                cols = st.multiselect("", info['dimensions'] + info['dates'], key="cols")
            with col3:
                st.markdown("**📌 Значения**")
                values = st.multiselect("", info['metrics'], key="values")
                if values:
                    agg = st.selectbox("Агрегация", ["SUM", "COUNT", "AVG", "MIN", "MAX"])

            with st.expander("⚙️ Дополнительные настройки"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    show_totals = st.checkbox("Показывать итоги", True)
                with c2:
                    show_percents = st.checkbox("Показывать проценты", False)
                with c3:
                    rank_values = st.checkbox("Ранжировать значения", False)

            if st.button("🔄 Построить сводную таблицу"):
                if values:
                    with st.spinner("Построение..."):
                        result, msg = pivot.create_pivot(rows, cols, values, agg if values else 'SUM',
                                                         show_totals, show_percents, rank_values)
                        if result is not None:
                            st.success(msg)
                            st.dataframe(result.style.background_gradient(cmap='Blues'), use_container_width=True, height=500)
                            st.session_state['current_pivot'] = result
                            # Статистика
                            total_sum = result.select_dtypes(include=['number']).sum().sum()
                            avg_value = result.select_dtypes(include=['number']).mean().mean()
                            st.markdown("### 📊 Статистика")
                            a, b, c, d = st.columns(4)
                            with a:
                                st.metric("Строк", len(result))
                            with b:
                                st.metric("Колонок", len(result.columns))
                            with c:
                                st.metric("Сумма", f"{total_sum:,.0f}")
                            with d:
                                st.metric("Среднее", f"{avg_value:,.2f}")
                        else:
                            st.error(msg)
                else:
                    st.warning("Выберите значения для анализа")

        # Вкладка 2 - Корреляция
        with tabs[1]:
            st.markdown("### 📈 Корреляционный анализ")
            if len(info['metrics']) >= 2:
                selected_metrics = st.multiselect(
                    "Выберите метрики",
                    info['metrics'],
                    default=info['metrics'][:min(3, len(info['metrics']))]
                )
                if len(selected_metrics) >= 2 and st.button("Построить корреляцию"):
                    with st.spinner("Расчет..."):
                        corr_matrix = pivot.correlation_analysis(selected_metrics)
                        if corr_matrix is not None:
                            fig = px.imshow(
                                corr_matrix,
                                text_auto=True,
                                title="Корреляционная матрица",
                                color_continuous_scale='RdBu',
                                zmin=-1,
                                zmax=1,
                                aspect='auto'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            # Вывод сильных корреляций
                            st.markdown("### 🔗 Сильные корреляции")
                            strong_corrs = []
                            for i in range(len(selected_metrics)):
                                for j in range(i+1, len(selected_metrics)):
                                    r = corr_matrix.iloc[i, j]
                                    if abs(r) > 0.5:
                                        strong_corrs.append({
                                            'Метрика 1': selected_metrics[i],
                                            'Метрика 2': selected_metrics[j],
                                            'Корреляция': f"{r:.3f}"
                                        })
                            if strong_corrs:
                                st.dataframe(pd.DataFrame(strong_corrs))
                            else:
                                st.info("Нет сильных корреляций")
            else:
                st.warning("Меньше двух метрик для анализа корреляции")

        # Вкладка 3 - Временные ряды
        with tabs[2]:
            st.markdown("### 📉 Анализ временных рядов")
            if info['dates']:
                col1, col2, col3 = st.columns(3)
                with col1:
                    date_col = st.selectbox("Дата", info['dates'])
                with col2:
                    metric_col = st.selectbox("Метрика", info['metrics'])
                with col3:
                    freq = st.selectbox("Период", ["День", "Неделя", "Месяц", "Квартал"])
                    freq_map = {"День": "day", "Неделя": "week", "Месяц": "month", "Квартал": "quarter"}
                if st.button("Построить ряд"):
                    with st.spinner("Построение..."):
                        ts_data = pivot.time_series(date_col, metric_col, freq_map[freq])
                        if ts_data is not None and not ts_data.empty:
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(
                                x=ts_data['period'],
                                y=ts_data['value'],
                                mode='lines+markers',
                                name='Факт',
                                line=dict(color='blue', width=2)
                            ))
                            if 'moving_avg_3' in ts_data:
                                fig.add_trace(go.Scatter(
                                    x=ts_data['period'],
                                    y=ts_data['moving_avg_3'],
                                    mode='lines',
                                    name='MA(3)',
                                    line=dict(color='orange', width=2, dash='dash')
                                ))
                            if 'trend' in ts_data:
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
                                hovermode='x',
                                height=500
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            if 'growth' in ts_data:
                                st.subheader("📈 Динамика роста")
                                growth_data = ts_data[['period', 'growth']].dropna()
                                fig_growth = px.bar(growth_data, x='period', y='growth',
                                                    title="Изменение в %",
                                                    color='growth',
                                                    color_continuous_scale='RdYlGn')
                                st.plotly_chart(fig_growth, use_container_width=True)
            else:
                st.warning("Нет колонок с датами")

        # Вкладка 4 - Прогнозы
        with tabs[3]:
            st.markdown("### 🔮 Прогнозирование")
            if info['dates']:
                col1, col2, col3 = st.columns(3)
                with col1:
                    date_col = st.selectbox("Дата", info['dates'], key="fc_date")
                with col2:
                    metric_col = st.selectbox("Метрика", info['metrics'], key="fc_metric")
                with col3:
                    periods = st.slider("Периодов прогноза", 3, 24, 12)
                if st.button("🔮 Построить прогноз", type="primary"):
                    with st.spinner("Прогнозирование..."):
                        forecast_data = pivot.forecast(date_col, metric_col, periods)
                        if forecast_data:
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(
                                x=forecast_data['historical']['period'],
                                y=forecast_data['historical']['value'],
                                mode='lines+markers',
                                name='История',
                                line=dict(color='blue', width=2)
                            ))
                            last_date = forecast_data['historical']['period'].iloc[-1]
                            future_dates = [last_date + timedelta(days=30*i) for i in range(1, periods+1)]
                            fig.add_trace(go.Scatter(
                                x=future_dates,
                                y=forecast_data['forecast'],
                                mode='lines+markers',
                                name='Прогноз',
                                line=dict(color='red', width=2, dash='dash')
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
                                title=f"Прогноз {metric_col}",
                                xaxis_title="Период",
                                yaxis_title=metric_col,
                                height=500
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            # Метрики прогноза
                            hist_values = forecast_data['historical']['value']
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Среднее", f"{hist_values.mean():.2f}")
                            with col2:
                                st.metric("Рост", f"{forecast_data['forecast'][-1] - hist_values.iloc[-1]:.2f}")
                            with col3:
                                st.metric("Волатильность", f"{hist_values.std():.2f}")
                            with col4:
                                growth_pct = ((forecast_data['forecast'][-1] - hist_values.iloc[-1]) / hist_values.iloc[-1]) * 100
                                st.metric("Рост %", f"{growth_pct:.1f}%")
            else:
                st.warning("Нет колонок с датами")

        # Вкладка 5 - Убрать раздел "Аномалии" (не отображаем)
        # Вкладка 6 - Сохранение отчета
        with tabs[4]:
            st.markdown("### 💾 Сохранение и экспорт")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 💾 Сохранить отчет")
                report_name = st.text_input("Название отчета", placeholder="Мой отчет")
                if st.button("Сохранить отчет") and 'current_pivot' in st.session_state:
                    config = {
                        'rows': st.session_state.get('rows', []),
                        'cols': st.session_state.get('cols', []),
                        'values': st.session_state.get('values', []),
                        'timestamp': datetime.now().isoformat()
                    }
                    if report_mgr.save(report_name or f"Отчет_{datetime.now().strftime('%Y%m%d_%H%M%S')}", config):
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
                        output = pd.ExcelWriter('temp.xlsx', engine='xlsxwriter')
                        result.to_excel(output, sheet_name='Pivot')
                        output.save()
                        with open('temp.xlsx', 'rb') as f:
                            st.download_button("📥 Скачать Excel", f, "pivot_report.xlsx")
                    elif format_type == "JSON":
                        json_data = result.to_json(orient='records', indent=2)
                        st.download_button("📥 Скачать JSON", json_data, "pivot_report.json")
            st.markdown("---")
            st.markdown("#### 📋 Сохраненные отчеты")
            reports = report_mgr.load_all()
            if not reports.empty:
                for _, report in reports.iterrows():
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"**{report['name']}**")
                        st.caption(pd.to_datetime(report['date']).strftime("%Y-%m-%d %H:%M"))
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
            <h1>🎯 Ultimate Pivot Analytics Pro</h1>
            <p style="font-size: 1.2em;">4 инструмента аналитики в одном месте</p>
            <br>
            <div style="display: flex; justify-content: center; gap: 20px; flex-wrap: wrap;">
                <div class="info-box" style="width: 200px;">
                    <h3>📊 Сводные таблицы</h3>
                </div>
                <div class="info-box" style="width: 200px;">
                    <h3>📈 Корреляции</h3>
                </div>
                <div class="info-box" style="width: 200px;">
                    <h3>📉 Временные ряды</h3>
                </div>
                <div class="info-box" style="width: 200px;">
                    <h3>🔮 Прогнозы</h3>
                </div>
            </div>
            <br>
            <p>🚀 Загрузите файл через боковую панель!</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
