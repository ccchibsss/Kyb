import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os
import glob
from datetime import datetime

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
        try:
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
        except Exception as e:
            st.error(f"Ошибка загрузки {file_path}: {str(e)}")
    
    return total_rows, f"✅ Загружено {total_rows:,} строк"

# ============================================
# 3. НАСТОЯЩАЯ СВОДНАЯ ТАБЛИЦА
# ============================================
class PivotTableBuilder:
    def __init__(self, conn, table_name='fact_sales'):
        self.conn = conn
        self.table_name = table_name
        
    def get_columns_info(self):
        """Получает список всех колонок и их типов"""
        try:
            # Проверяем существование таблицы
            tables = self.conn.execute("SHOW TABLES").fetchdf()
            if self.table_name not in tables['name'].values:
                return []
            
            # Получаем структуру таблицы
            df_sample = self.conn.execute(f"SELECT * FROM {self.table_name} LIMIT 1000").fetchdf()
            
            columns_info = []
            for col in df_sample.columns:
                # Определяем числовые колонки
                is_numeric = pd.api.types.is_numeric_dtype(df_sample[col])
                
                columns_info.append({
                    'name': col,
                    'is_numeric': is_numeric,
                    'dtype': str(df_sample[col].dtype),
                    'unique_count': df_sample[col].nunique()
                })
            
            return columns_info
        except Exception as e:
            st.error(f"Ошибка получения структуры: {str(e)}")
            return []
    
    def create_pivot(self, rows, columns, values, agg_func='SUM'):
        """Создаёт сводную таблицу как в Excel"""
        
        if not values:
            return None, "Выберите значения для анализа"
        
        try:
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
                
                # Создаём Pivot Table (транспонирование)
                if columns and len(columns) > 0 and len(result) > 0:
                    # Для одной метрики
                    if len(values) == 1:
                        pivot_df = result.pivot_table(
                            index=rows if rows else None,
                            columns=columns,
                            values=values[0],
                            aggfunc=agg_func.lower(),
                            fill_value=0
                        )
                    else:
                        # Для нескольких метрик
                        pivot_dfs = []
                        for value in values:
                            temp_pivot = result.pivot_table(
                                index=rows if rows else None,
                                columns=columns,
                                values=value,
                                aggfunc=agg_func.lower(),
                                fill_value=0
                            )
                            # Добавляем префикс с названием метрики
                            temp_pivot.columns = [f"{value} - {col}" for col in temp_pivot.columns]
                            pivot_dfs.append(temp_pivot)
                        
                        pivot_df = pd.concat(pivot_dfs, axis=1)
                    
                    return pivot_df, f"✅ Сводная таблица: {len(pivot_df)} строк × {len(pivot_df.columns)} столбцов"
                else:
                    return result, f"✅ Данные сгруппированы: {len(result)} строк"
            else:
                # Простая агрегация без группировки
                agg_exprs = [f"{agg_func}({v}) as {v}" for v in values]
                query = f"SELECT {', '.join(agg_exprs)} FROM {self.table_name}"
                result = self.conn.execute(query).fetchdf()
                return result, "✅ Агрегированные данные"
                
        except Exception as e:
            return None, f"❌ Ошибка: {str(e)}"

# ============================================
# 4. ОСНОВНОЙ ИНТЕРФЕЙС
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
                
                with st.spinner("Загрузка данных..."):
                    rows, msg = load_excel_to_duckdb(conn, temp_files)
                    st.success(msg)
                
                for f in temp_files:
                    try:
                        os.remove(f)
                    except:
                        pass
                st.rerun()
        
        st.divider()
        
        # Информация о данных
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
            st.metric("📊 Всего записей", f"{row_count:,}")
            
            # Показываем список таблиц
            tables = conn.execute("SHOW TABLES").fetchdf()
            if not tables.empty:
                with st.expander("📋 Таблицы в БД"):
                    st.dataframe(tables, use_container_width=True)
        except:
            st.info("ℹ️ Нет данных")
            row_count = 0
        
        st.divider()
        
        if st.button("🗑️ Очистить все данные", use_container_width=True):
            try:
                conn.execute("DROP TABLE IF EXISTS fact_sales")
                conn.execute("DROP TABLE IF EXISTS load_history")
                st.success("Данные очищены")
                st.rerun()
            except Exception as e:
                st.error(f"Ошибка: {str(e)}")
    
    # ============================================
    # ОСНОВНОЙ ЭКРАН - КОНСТРУКТОР СВОДНОЙ ТАБЛИЦЫ
    # ============================================
    
    if row_count > 0:
        # Получаем колонки
        columns_info = pivot_builder.get_columns_info()
        
        if columns_info:
            # Разделяем на измерения и факты
            dimensions = [col['name'] for col in columns_info if not col['is_numeric']]
            measures = [col['name'] for col in columns_info if col['is_numeric']]
            
            st.subheader("🎯 Настройка сводной таблицы")
            st.markdown("*Выберите поля в каждой области*")
            
            # Три колонки как в Excel
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 📊 СТРОКИ")
                rows = st.multiselect(
                    "Поля для строк (иерархия)",
                    options=dimensions,
                    key="rows_select",
                    placeholder="Выберите поля для строк..."
                )
                
                # Показываем выбранные поля
                if rows:
                    st.markdown("**Выбранные поля:**")
                    for i, r in enumerate(rows, 1):
                        st.markdown(f"{i}. 📍 **{r}**")
            
            with col2:
                st.markdown("### 📈 КОЛОНКИ")
                columns = st.multiselect(
                    "Поля для колонок",
                    options=dimensions,
                    key="columns_select",
                    placeholder="Выберите поля для колонок..."
                )
                
                if columns:
                    st.markdown("**Выбранные поля:**")
                    for i, c in enumerate(columns, 1):
                        st.markdown(f"{i}. 📌 **{c}**")
            
            with col3:
                st.markdown("### 🧮 ЗНАЧЕНИЯ")
                values = st.multiselect(
                    "Числовые поля для анализа",
                    options=measures,
                    key="values_select",
                    placeholder="Выберите числовые поля..."
                )
                
                if values:
                    agg_func = st.selectbox(
                        "📐 Функция агрегации",
                        ["SUM", "COUNT", "AVG", "MIN", "MAX"],
                        key="agg_func"
                    )
                    
                    st.markdown("**Выбранные поля:**")
                    for i, v in enumerate(values, 1):
                        st.markdown(f"{i}. 💹 **{v}** ({agg_func})")
            
            st.markdown("---")
            
            # Кнопка обновления
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                update_button = st.button(
                    "🔄 ПОСТРОИТЬ СВОДНУЮ ТАБЛИЦУ",
                    type="primary",
                    use_container_width=True
                )
            
            # Отображаем результат
            if update_button or (rows or columns or values):
                if values:
                    with st.spinner("Построение сводной таблицы..."):
                        result_df, message = pivot_builder.create_pivot(rows, columns, values, agg_func)
                        
                        if result_df is not None:
                            st.success(message)
                            
                            # Показываем результат
                            st.subheader("📋 РЕЗУЛЬТАТ СВОДНОЙ ТАБЛИЦЫ")
                            
                            # Настройки отображения
                            with st.expander("⚙️ Настройки отображения"):
                                col_settings1, col_settings2 = st.columns(2)
                                with col_settings1:
                                    show_totals = st.checkbox("Показывать итоги", value=True)
                                    number_format = st.checkbox("Форматировать числа", value=True)
                                with col_settings2:
                                    precision = st.slider("Точность чисел", 0, 4, 2)
                            
                            # Форматируем
                            if number_format:
                                styled_df = result_df.style.format(f"{{:,.{precision}f}}")
                            else:
                                styled_df = result_df
                            
                            # Показываем таблицу
                            st.dataframe(styled_df, use_container_width=True, height=500)
                            
                            # Итоги
                            if show_totals and len(result_df) > 0:
                                st.markdown("### 📊 ИТОГИ")
                                
                                tab1, tab2 = st.tabs(["Итоги по строкам", "Итоги по колонкам"])
                                
                                with tab1:
                                    if rows:
                                        row_totals = result_df.sum(axis=1).sort_values(ascending=False)
                                        st.dataframe(
                                            pd.DataFrame({
                                                'Категория': row_totals.index,
                                                'Сумма': row_totals.values
                                            }).head(20),
                                            use_container_width=True
                                        )
                                    else:
                                        st.info("Нет полей в строках")
                                
                                with tab2:
                                    if columns:
                                        col_totals = result_df.sum(axis=0).sort_values(ascending=False)
                                        st.dataframe(
                                            pd.DataFrame({
                                                'Категория': col_totals.index,
                                                'Сумма': col_totals.values
                                            }).head(20),
                                            use_container_width=True
                                        )
                                    else:
                                        st.info("Нет полей в колонках")
                            
                            # Визуализация
                            st.subheader("📊 ВИЗУАЛИЗАЦИЯ")
                            
                            # Выбираем метрику для визуализации
                            if len(result_df.columns) > 0:
                                # Определяем числовые колонки
                                numeric_cols = result_df.select_dtypes(include=['number']).columns
                                
                                if len(numeric_cols) > 0:
                                    chart_col1, chart_col2 = st.columns(2)
                                    
                                    with chart_col1:
                                        chart_type = st.selectbox(
                                            "Тип графика",
                                            ["Столбчатая диаграмма", "Линейный график", "Тепловая карта", "Круговая диаграмма"]
                                        )
                                    
                                    with chart_col2:
                                        metric_for_chart = st.selectbox(
                                            "Метрика для отображения",
                                            numeric_cols
                                        )
                                    
                                    # Строим график
                                    if chart_type == "Столбчатая диаграмма":
                                        plot_data = result_df[metric_for_chart].head(20)
                                        fig = px.bar(
                                            x=plot_data.index,
                                            y=plot_data.values,
                                            title=f"{metric_for_chart} - Столбчатая диаграмма",
                                            labels={'x': 'Категория', 'y': metric_for_chart},
                                            color_discrete_sequence=['#FF4B4B']
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                    
                                    elif chart_type == "Линейный график":
                                        plot_data = result_df[metric_for_chart]
                                        fig = px.line(
                                            x=plot_data.index,
                                            y=plot_data.values,
                                            title=f"{metric_for_chart} - Тренд",
                                            markers=True
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                                    
                                    elif chart_type == "Тепловая карта":
                                        if len(result_df) > 1 and len(result_df.columns) > 1:
                                            fig = px.imshow(
                                                result_df[numeric_cols].values,
                                                x=numeric_cols,
                                                y=result_df.index,
                                                title="Тепловая карта данных",
                                                color_continuous_scale='RdBu',
                                                aspect="auto"
                                            )
                                            st.plotly_chart(fig, use_container_width=True)
                                        else:
                                            st.warning("Для тепловой карты нужно больше данных")
                                    
                                    elif chart_type == "Круговая диаграмма":
                                        plot_data = result_df[metric_for_chart].head(10)
                                        fig = px.pie(
                                            values=plot_data.values,
                                            names=plot_data.index,
                                            title=f"Распределение {metric_for_chart}"
                                        )
                                        st.plotly_chart(fig, use_container_width=True)
                            
                            # Экспорт
                            st.subheader("💾 ЭКСПОРТ")
                            
                            csv_data = result_df.to_csv()
                            st.download_button(
                                label="📥 Скачать как CSV",
                                data=csv_data,
                                file_name=f"pivot_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                use_container_width=True
                            )
                            
                            # Дополнительная информация
                            with st.expander("ℹ️ Информация о данных"):
                                st.metric("Количество строк", len(result_df))
                                st.metric("Количество столбцов", len(result_df.columns))
                                st.write("**Структура данных:**")
                                st.dataframe(result_df.dtypes.astype(str).reset_index().rename(columns={'index': 'Колонка', 0: 'Тип'}), use_container_width=True)
                        else:
                            st.error(message)
                else:
                    st.info("💡 **Выберите хотя бы одно поле в область ЗНАЧЕНИЯ** для построения сводной таблицы")
    else:
        # Показываем приветствие если нет данных
        st.info("""
        # 🎯 Добро пожаловать в конструктор сводных таблиц!
        
        ## Как это работает:
        
        1. **Загрузите Excel файлы** через боковую панель слева
        2. **Выберите поля** в трёх областях:
           - 📊 **СТРОКИ** - категории для группировки по вертикали
           - 📈 **КОЛОНКИ** - категории для транспонирования
           - 🧮 **ЗНАЧЕНИЯ** - числовые поля для анализа
        3. **Нажмите кнопку** "Построить сводную таблицу"
        4. **Анализируйте результат** и меняйте настройки
        
        💡 *Полностью как в Excel, но с мощью DuckDB под капотом!*
        """)
        
        # Пример
        with st.expander("📖 Пример структуры Excel файла"):
            st.markdown("""
            Ваш Excel файл должен содержать данные в табличном формате, например:
            
            | Дата       | Продукт | Категория | Продажи | Количество |
            |------------|---------|-----------|---------|-------------|
            | 2024-01-01 | Ноутбук | Электроника| 50000   | 10          |
            | 2024-01-02 | Мышь    | Аксессуары | 1500    | 30          |
            
            - **СТРОКИ/КОЛОНКИ**: текстовые поля (Дата, Продукт, Категория)
            - **ЗНАЧЕНИЯ**: числовые поля (Продажи, Количество)
            """)

if __name__ == "__main__":
    main()
