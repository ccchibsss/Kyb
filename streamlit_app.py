import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os
import glob
from datetime import datetime
from streamlit_sortables import sort_items

# ============================================
# 1. ПОДКЛЮЧЕНИЕ К DUCKDB
# ============================================
@st.cache_resource
def get_duckdb_connection():
    """Создаёт подключение к DuckDB"""
    conn = duckdb.connect('olap_cube.duckdb')
    
    # Создаём таблицу для логирования
    conn.execute("""
        CREATE TABLE IF NOT EXISTS load_history (
            id INTEGER PRIMARY KEY,
            load_date TIMESTAMP,
            file_path VARCHAR,
            rows_loaded INTEGER,
            status VARCHAR
        )
    """)
    
    return conn

# ============================================
# 2. РАБОТА С ФАЙЛАМИ
# ============================================
def load_excel_to_duckdb(conn, file_paths, table_name='fact_sales'):
    """Загружает Excel файлы в DuckDB"""
    if not file_paths:
        return 0, "Нет файлов"
    
    total_rows = 0
    first_file = True
    
    for file_path in file_paths:
        df = pd.read_excel(file_path)
        
        if first_file:
            conn.register('temp_df', df)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
            first_file = False
        else:
            conn.register('temp_df', df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        
        total_rows += len(df)
        
        conn.execute("""
            INSERT INTO load_history (load_date, file_path, rows_loaded, status)
            VALUES (CURRENT_TIMESTAMP, ?, ?, 'SUCCESS')
        """, [str(file_path), len(df)])
    
    return total_rows, f"✅ Загружено {total_rows:,} строк"

# ============================================
# 3. НАСТОЯЩАЯ СВОДНАЯ ТАБЛИЦА (PIVOT TABLE)
# ============================================
class PivotTableBuilder:
    def __init__(self, conn, table_name='fact_sales'):
        self.conn = conn
        self.table_name = table_name
        
    def get_columns_info(self):
        """Получает список всех колонок и их типов"""
        try:
            # Получаем структуру таблицы
            df_sample = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 1").fetchdf()
            
            columns_info = []
            for col in df_sample.columns:
                # Определяем тип колонки по первым 100 значениям
                sample = self.conn.execute(f"SELECT {col} FROM {self.table_name} LIMIT 100").fetchdf()
                
                # Пытаемся определить числовые колонки
                is_numeric = pd.to_numeric(sample[col], errors='coerce').notna().any()
                
                columns_info.append({
                    'name': col,
                    'is_numeric': is_numeric,
                    'dtype': str(df_sample[col].dtype)
                })
            
            return columns_info
        except:
            return []
    
    def create_pivot(self, rows, columns, values, agg_func='SUM'):
        """Создаёт сводную таблицу"""
        
        if not values:
            return None, "Выберите значения для анализа"
        
        # Строим запрос с группировкой
        group_by = rows + columns
        
        if group_by:
            # Агрегируем данные
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
            
            # Создаём Pivot Table
            if columns and len(result) > 0:
                # Создаём сводную таблицу
                pivot_df = result.pivot_table(
                    index=rows if rows else None,
                    columns=columns,
                    values=values[0] if len(values) == 1 else values,
                    aggfunc=agg_func.lower()
                )
                
                # Заполняем пропуски
                pivot_df = pivot_df.fillna(0)
                
                return pivot_df, f"✅ Сводная таблица: {len(pivot_df)} строк × {len(pivot_df.columns)} столбцов"
            else:
                return result, "✅ Данные сгруппированы (без транспонирования)"
        else:
            # Простая агрегация без группировки
            agg_exprs = [f"{agg_func}({v}) as {v}" for v in values]
            query = f"SELECT {', '.join(agg_exprs)} FROM {self.table_name}"
            result = self.conn.execute(query).fetchdf()
            return result, "✅ Агрегированные данные"

# ============================================
# 4. ИНТЕРФЕЙС КАК В EXCEL
# ============================================
def main():
    st.set_page_config(
        page_title="Excel-style Pivot Table",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Конструктор сводных таблиц (как в Excel)")
    st.markdown("---")
    
    # Подключаемся к БД
    conn = get_duckdb_connection()
    pivot_builder = PivotTableBuilder(conn)
    
    # ============================================
    # SIDEBAR - ЗАГРУЗКА ДАННЫХ
    # ============================================
    with st.sidebar:
        st.header("📁 Загрузка данных")
        
        uploaded_files = st.file_uploader(
            "Загрузите Excel файлы",
            type=['xlsx', 'xls'],
            accept_multiple_files=True
        )
        
        if uploaded_files:
            if st.button("🚀 Загрузить в БД", type="primary", use_container_width=True):
                temp_files = []
                for f in uploaded_files:
                    temp_path = f"temp_{f.name}"
                    with open(temp_path, "wb") as out:
                        out.write(f.getbuffer())
                    temp_files.append(temp_path)
                
                with st.spinner("Загрузка..."):
                    rows, msg = load_excel_to_duckdb(conn, temp_files)
                    st.success(msg)
                
                for f in temp_files:
                    os.remove(f)
                st.rerun()
        
        st.divider()
        
        # Информация о данных
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
            st.metric("📊 Всего записей", f"{row_count:,}")
        except:
            st.info("ℹ️ Нет данных")
            row_count = 0
        
        if st.button("🗑️ Очистить", use_container_width=True):
            conn.execute("DROP TABLE IF EXISTS fact_sales")
            st.rerun()
    
    # ============================================
    # ОСНОВНОЙ ЭКРАН - КОНСТРУКТОР СВОДНОЙ ТАБЛИЦЫ
    # ============================================
    
    if row_count > 0:
        # Получаем колонки
        columns_info = pivot_builder.get_columns_info()
        
        if columns_info:
            # Разделяем на измерения и факты
            all_columns = [col['name'] for col in columns_info]
            dimensions = [col['name'] for col in columns_info if not col['is_numeric']]
            measures = [col['name'] for col in columns_info if col['is_numeric']]
            
            # Стилизация как в Excel
            st.markdown("""
            <style>
            .pivot-area {
                background-color: #f8f9fa;
                border-radius: 10px;
                padding: 20px;
                margin: 10px 0;
                border: 2px solid #dee2e6;
            }
            .pivot-area h4 {
                margin-top: 0;
                color: #495057;
            }
            .field-list {
                background-color: white;
                border-radius: 5px;
                padding: 10px;
                min-height: 200px;
                border: 1px solid #ced4da;
            }
            .field-item {
                background-color: #e9ecef;
                margin: 5px;
                padding: 5px 10px;
                border-radius: 5px;
                cursor: pointer;
                display: inline-block;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # ИНТЕРФЕЙС КАК В EXCEL - 4 ОБЛАСТИ
            st.subheader("🎯 Перетащите поля в нужные области")
            
            # Верхняя строка - Фильтры (опционально)
            with st.expander("🔍 Фильтры (опционально)"):
                filters = st.multiselect(
                    "Поля для фильтрации",
                    options=all_columns,
                    key="filters"
                )
                
                if filters:
                    for f in filters:
                        unique_vals = conn.execute(f"SELECT DISTINCT {f} FROM fact_sales LIMIT 20").fetchdf()
                        selected = st.multiselect(f"Фильтр: {f}", unique_vals[f].tolist())
                        if selected:
                            # Применяем фильтр (упрощённо)
                            pass
            
            # Основные 3 области сводной
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown('<div class="pivot-area">', unsafe_allow_html=True)
                st.markdown("### 📊 СТРОКИ")
                st.markdown("*Перетащите сюда поля для строк*")
                rows = st.multiselect(
                    "Поля строк",
                    options=dimensions,
                    key="rows_area",
                    label_visibility="collapsed",
                    placeholder="Выберите поля для строк..."
                )
                if rows:
                    for r in rows:
                        st.markdown(f"📌 **{r}**")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="pivot-area">', unsafe_allow_html=True)
                st.markdown("### 📈 КОЛОНКИ")
                st.markdown("*Перетащите сюда поля для колонок*")
                columns = st.multiselect(
                    "Поля колонок",
                    options=dimensions,
                    key="columns_area",
                    label_visibility="collapsed",
                    placeholder="Выберите поля для колонок..."
                )
                if columns:
                    for c in columns:
                        st.markdown(f"📍 **{c}**")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown('<div class="pivot-area">', unsafe_allow_html=True)
                st.markdown("### 🧮 ЗНАЧЕНИЯ")
                st.markdown("*Перетащите сюда поля для расчётов*")
                values = st.multiselect(
                    "Поля значений",
                    options=measures,
                    key="values_area",
                    label_visibility="collapsed",
                    placeholder="Выберите числовые поля..."
                )
                
                if values:
                    agg_func = st.selectbox(
                        "Функция агрегации",
                        ["SUM", "COUNT", "AVG", "MIN", "MAX"],
                        key="aggregation"
                    )
                    for v in values:
                        st.markdown(f"💹 **{v}** ({agg_func})")
                st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown("---")
            
            # КНОПКА ОБНОВЛЕНИЯ
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                update_button = st.button(
                    "🔄 ОБНОВИТЬ СВОДНУЮ ТАБЛИЦУ",
                    type="primary",
                    use_container_width=True
                )
            
            # ПОКАЗЫВАЕМ РЕЗУЛЬТАТ
            if update_button or (rows or columns or values):
                if values:
                    with st.spinner("Построение сводной таблицы..."):
                        result, msg = pivot_builder.create_pivot(rows, columns, values, agg_func)
                        
                        if result is not None:
                            st.success(msg)
                            
                            # Отображение результата
                            st.subheader("📋 РЕЗУЛЬТАТ СВОДНОЙ ТАБЛИЦЫ")
                            
                            # Настройки отображения
                            col_format, col_viz = st.columns([1, 2])
                            with col_format:
                                show_totals = st.checkbox("Показывать итоги", value=True)
                                format_numbers = st.checkbox("Форматировать числа", value=True)
                            
                            # Форматируем
                            if format_numbers:
                                styled_result = result.style.format("{:,.2f}")
                            else:
                                styled_result = result
                            
                            # Показываем таблицу
                            st.dataframe(styled_result, use_container_width=True, height=500)
                            
                            # Добавляем итоги
                            if show_totals and len(result) > 0:
                                st.markdown("### 📊 ИТОГИ")
                                
                                # Итоги по строкам
                                if rows:
                                    st.markdown("**Итоги по строкам:**")
                                    row_totals = result.sum(axis=1).sort_values(ascending=False).head(10)
                                    st.dataframe(pd.DataFrame({
                                        'Категория': row_totals.index,
                                        'Общая сумма': row_totals.values
                                    }), use_container_width=True)
                                
                                # Итоги по колонкам
                                if columns:
                                    st.markdown("**Итоги по колонкам:**")
                                    col_totals = result.sum(axis=0).sort_values(ascending=False).head(10)
                                    st.dataframe(pd.DataFrame({
                                        'Категория': col_totals.index,
                                        'Общая сумма': col_totals.values
                                    }), use_container_width=True)
                            
                            # ВИЗУАЛИЗАЦИЯ
                            st.subheader("📊 ВИЗУАЛИЗАЦИЯ")
                            
                            viz_col1, viz_col2 = st.columns(2)
                            
                            with viz_col1:
                                chart_type = st.selectbox(
                                    "Тип графика",
                                    ["Столбчатая диаграмма", "Линейный график", "Тепловая карта", "Круговая диаграмма"]
                                )
                            
                            with viz_col2:
                                if len(result.columns) > 1:
                                    chart_metric = st.selectbox("Метрика для отображения", result.columns)
                                else:
                                    chart_metric = result.columns[0] if len(result.columns) > 0 else None
                            
                            if chart_metric:
                                if chart_type == "Столбчатая диаграмма":
                                    # Берём топ-20 для читаемости
                                    plot_data = result[chart_metric].head(20)
                                    fig = px.bar(
                                        x=plot_data.index,
                                        y=plot_data.values,
                                        title=f"{chart_metric} - Столбчатая диаграмма",
                                        labels={'x': 'Категория', 'y': chart_metric}
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                
                                elif chart_type == "Линейный график":
                                    plot_data = result[chart_metric]
                                    fig = px.line(
                                        x=plot_data.index,
                                        y=plot_data.values,
                                        title=f"{chart_metric} - Тренд",
                                        markers=True
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                
                                elif chart_type == "Тепловая карта":
                                    # Для тепловой карты нужно 2D
                                    if len(result.columns) > 1 and len(result) > 1:
                                        fig = px.imshow(
                                            result.values,
                                            x=result.columns,
                                            y=result.index,
                                            title="Тепловая карта данных",
                                            color_continuous_scale='RdYlGn',
                                            aspect="auto"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                    else:
                                        st.warning("Для тепловой карты нужно больше измерений")
                                
                                elif chart_type == "Круговая диаграмма":
                                    plot_data = result[chart_metric].head(10)
                                    fig = px.pie(
                                        values=plot_data.values,
                                        names=plot_data.index,
                                        title=f"Распределение {chart_metric}"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                            
                            # ЭКСПОРТ
                            st.subheader("💾 ЭКСПОРТ")
                            csv = result.to_csv()
                            st.download_button(
                                label="📥 Скачать как CSV",
                                data=csv,
                                file_name=f"pivot_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                        else:
                            st.error(msg)
                else:
                    st.info("💡 Выберите хотя бы одно поле в область ЗНАЧЕНИЯ")
    else:
        st.info("📂 **Начните с загрузки данных**\n\nИспользуйте боковую панель слева, чтобы загрузить Excel файлы")

if __name__ == "__main__":
    main()
