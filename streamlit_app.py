import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os
from pathlib import Path
import glob
from datetime import datetime

# ============================================
# 1. НАСТРОЙКА ПОДКЛЮЧЕНИЯ К DUCKDB
# ============================================
@st.cache_resource
def get_duckdb_connection():
    """Создаёт или открывает persistent-базу данных"""
    conn = duckdb.connect('olap_cube.duckdb')
    
    conn.execute("INSTALL spatial IF NOT EXISTS")
    conn.execute("LOAD spatial")
    
    # Создаём таблицу для логирования обновлений
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
# 2. ФУНКЦИИ ДЛЯ РАБОТЫ С ФАЙЛАМИ
# ============================================
def get_excel_files_from_directory(directory_path):
    """Получает список всех Excel-файлов в директории"""
    excel_extensions = ['*.xlsx', '*.xls', '*.xlsm']
    files = []
    for ext in excel_extensions:
        files.extend(glob.glob(os.path.join(directory_path, ext)))
    return files

def preview_excel_file(file_path, rows=100):
    """Показывает предпросмотр Excel файла"""
    try:
        df = pd.read_excel(file_path, nrows=rows)
        return df, None
    except Exception as e:
        return None, str(e)

def load_excel_files_to_duckdb(conn, file_paths, table_name='fact_sales'):
    """Загружает выбранные Excel-файлы в DuckDB"""
    
    if not file_paths:
        return 0, "Нет файлов для загрузки"
    
    total_rows = 0
    first_file = True
    
    try:
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
            """, [file_path, len(df)])
        
        return total_rows, f"✅ Успешно загружено {total_rows:,} строк из {len(file_paths)} файлов"
    
    except Exception as e:
        return 0, f"❌ Ошибка при загрузке: {str(e)}"

def append_excel_files_to_duckdb(conn, file_paths, table_name='fact_sales'):
    """Добавляет новые файлы к существующей таблице"""
    
    if not file_paths:
        return 0, "Нет файлов для добавления"
    
    total_rows = 0
    
    try:
        for file_path in file_paths:
            df = pd.read_excel(file_path)
            conn.register('temp_df', df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
            total_rows += len(df)
            
            conn.execute("""
                INSERT INTO load_history (load_date, file_path, rows_loaded, status)
                VALUES (CURRENT_TIMESTAMP, ?, ?, 'APPEND')
            """, [file_path, len(df)])
        
        return total_rows, f"✅ Добавлено {total_rows:,} строк из {len(file_paths)} файлов"
    
    except Exception as e:
        return 0, f"❌ Ошибка при добавлении: {str(e)}"

def get_load_history(conn):
    """Получает историю загрузок"""
    return conn.execute("""
        SELECT 
            load_date,
            file_path,
            rows_loaded,
            status
        FROM load_history 
        ORDER BY load_date DESC
        LIMIT 50
    """).fetchdf()

# ============================================
# 3. OLAP-ФУНКЦИИ С ПОДДЕРЖКОЙ АГРЕГАЦИЙ
# ============================================
def get_column_info(conn, table_name='fact_sales'):
    """Получает информацию о колонках таблицы"""
    columns_df = conn.execute(f"DESCRIBE {table_name}").fetchdf()
    
    numeric_types = ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'REAL']
    
    columns = []
    for _, row in columns_df.iterrows():
        col_name = row['column_name']
        col_type = row['column_type'].upper()
        is_numeric = any(nt in col_type for nt in numeric_types)
        
        columns.append({
            'name': col_name,
            'type': col_type,
            'is_numeric': is_numeric,
            'sample': conn.execute(f"SELECT {col_name} FROM {table_name} LIMIT 1").fetchone()[0] if is_numeric else None
        })
    
    return columns

def build_pivot_query(conn, rows, columns, values, agg_func='SUM', fact_table='fact_sales'):
    """Строит запрос для сводной таблицы с поддержкой иерархий"""
    
    if not values:
        return None, "Выберите хотя бы одно значение для анализа"
    
    # Определяем группировку
    group_by_fields = rows + columns
    
    if not group_by_fields:
        # Если нет ни строк, ни колонок - просто агрегируем всё
        agg_fields = [f"{agg_func}({value}) as {value}" for value in values]
        query = f"""
            SELECT 
                {', '.join(agg_fields)}
            FROM {fact_table}
        """
    else:
        # Строим обычную группировку
        agg_fields = [f"{agg_func}({value}) as {value}" for value in values]
        query = f"""
            SELECT 
                {', '.join(group_by_fields)},
                {', '.join(agg_fields)}
            FROM {fact_table}
            GROUP BY {', '.join(group_by_fields)}
            ORDER BY {', '.join(group_by_fields)}
        """
    
    return query, None

def run_pivot_query(conn, rows, columns, values, agg_func='SUM', fact_table='fact_sales'):
    """Выполняет запрос сводной таблицы и возвращает результат"""
    
    query, error = build_pivot_query(conn, rows, columns, values, agg_func, fact_table)
    
    if error:
        return None, error
    
    try:
        result = conn.execute(query).fetchdf()
        
        # Если есть колонки, делаем pivot (транспонирование)
        if columns and len(columns) > 0 and len(result) > 0:
            try:
                # Создаём pivot таблицу
                if len(values) == 1:
                    pivot_df = result.pivot_table(
                        index=rows if rows else None,
                        columns=columns,
                        values=values[0],
                        aggfunc='sum'
                    )
                else:
                    # Для нескольких значений создаём мультииндекс
                    pivot_dfs = []
                    for value in values:
                        temp_pivot = result.pivot_table(
                            index=rows if rows else None,
                            columns=columns,
                            values=value,
                            aggfunc='sum'
                        )
                        temp_pivot.columns = [f"{value} - {col}" for col in temp_pivot.columns]
                        pivot_dfs.append(temp_pivot)
                    
                    pivot_df = pd.concat(pivot_dfs, axis=1)
                
                return pivot_df.fillna(0), "✅ Сводная таблица создана"
            except Exception as e:
                # Если pivot не удался, возвращаем обычную таблицу
                return result, f"⚠️ Сводная таблица создана в плоском формате: {str(e)}"
        else:
            return result, "✅ Данные загружены"
            
    except Exception as e:
        return None, f"❌ Ошибка: {str(e)}"

# ============================================
# 4. ИНТЕРФЕЙС СВОДНОЙ ТАБЛИЦЫ
# ============================================
def render_pivot_builder(conn):
    """Рендерит интерфейс конструктора сводной таблицы"""
    
    # Получаем информацию о колонках
    columns_info = get_column_info(conn)
    
    if not columns_info:
        st.warning("Нет данных для анализа")
        return None, None, None, None
    
    # Разделяем колонки на измерения и значения
    dimensions = [col['name'] for col in columns_info if not col['is_numeric']]
    metrics = [col['name'] for col in columns_info if col['is_numeric']]
    
    # Доступные агрегации
    aggregations = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX']
    
    # Создаём три колонки для интерфейса перетаскивания
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 📊 Строки")
        st.markdown("*Перетащите поля сюда*")
        rows = st.multiselect(
            "Поля строк",
            options=dimensions + metrics,
            key="rows_select",
            label_visibility="collapsed"
        )
        
        if rows:
            st.markdown("**Выбранные поля:**")
            for r in rows:
                st.markdown(f"- 📍 {r}")
    
    with col2:
        st.markdown("### 📈 Колонки")
        st.markdown("*Перетащите поля сюда*")
        columns = st.multiselect(
            "Поля колонок",
            options=dimensions + metrics,
            key="columns_select",
            label_visibility="collapsed"
        )
        
        if columns:
            st.markdown("**Выбранные поля:**")
            for c in columns:
                st.markdown(f"- 📌 {c}")
    
    with col3:
        st.markdown("### 🧮 Значения")
        st.markdown("*Перетащите поля сюда*")
        values = st.multiselect(
            "Поля значений",
            options=metrics,
            key="values_select",
            label_visibility="collapsed"
        )
        
        if values:
            st.markdown("**Агрегация:**")
            agg_func = st.selectbox(
                "Выберите функцию агрегации",
                aggregations,
                key="agg_select"
            )
            st.markdown("**Выбранные поля:**")
            for v in values:
                st.markdown(f"- 💹 {v} ({agg_func})")
    
    return rows, columns, values, agg_func if values else None

# ============================================
# 5. ОСНОВНОЙ ИНТЕРФЕЙС
# ============================================
def main():
    st.set_page_config(
        page_title="OLAP-куб на DuckDB",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Интерактивная сводная таблица на DuckDB")
    st.markdown("""
    ### Конструктор сводной таблицы
    Выбирайте поля для строк, колонок и значений — анализ обновляется мгновенно!
    """)
    
    # Подключаемся к DuckDB
    conn = get_duckdb_connection()
    
    # ============================================
    # 6. БОКОВАЯ ПАНЕЛЬ - УПРАВЛЕНИЕ ДАННЫМИ
    # ============================================
    with st.sidebar:
        st.header("📁 Управление данными")
        
        # Выбор метода загрузки
        load_method = st.radio(
            "Режим загрузки",
            ["📂 Выбрать папку", "📄 Выбрать файлы", "➕ Добавить файлы", "🔍 Предпросмотр файлов"]
        )
        
        if load_method == "📂 Выбрать папку":
            folder_path = st.text_input(
                "Путь к папке с Excel-файлами",
                placeholder="C:/Users/YourName/Documents/excel_files"
            )
            
            if folder_path and os.path.exists(folder_path):
                excel_files = get_excel_files_from_directory(folder_path)
                if excel_files:
                    st.success(f"Найдено {len(excel_files)} Excel-файлов")
                    
                    if st.button("🚀 Загрузить все файлы", type="primary", use_container_width=True):
                        with st.spinner("Загрузка данных..."):
                            rows, message = load_excel_files_to_duckdb(conn, excel_files)
                            st.success(message)
                            st.rerun()
                else:
                    st.warning("Нет Excel-файлов")
        
        elif load_method == "📄 Выбрать файлы":
            uploaded_files = st.file_uploader(
                "Выберите Excel-файлы",
                type=['xlsx', 'xls', 'xlsm'],
                accept_multiple_files=True
            )
            
            if uploaded_files and st.button("🚀 Загрузить", type="primary", use_container_width=True):
                temp_files = []
                for uploaded_file in uploaded_files:
                    temp_path = f"temp_{uploaded_file.name}"
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    temp_files.append(temp_path)
                
                with st.spinner("Загрузка..."):
                    rows, message = load_excel_files_to_duckdb(conn, temp_files)
                    st.success(message)
                    for temp_file in temp_files:
                        os.remove(temp_file)
                    st.rerun()
        
        elif load_method == "➕ Добавить файлы":
            uploaded_files = st.file_uploader(
                "Выберите файлы для добавления",
                type=['xlsx', 'xls', 'xlsm'],
                accept_multiple_files=True
            )
            
            if uploaded_files and st.button("➕ Добавить", type="primary", use_container_width=True):
                temp_files = []
                for uploaded_file in uploaded_files:
                    temp_path = f"temp_{uploaded_file.name}"
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    temp_files.append(temp_path)
                
                with st.spinner("Добавление..."):
                    rows, message = append_excel_files_to_duckdb(conn, temp_files)
                    st.success(message)
                    for temp_file in temp_files:
                        os.remove(temp_file)
                    st.rerun()
        
        elif load_method == "🔍 Предпросмотр файлов":
            uploaded_file = st.file_uploader(
                "Выберите Excel файл для предпросмотра",
                type=['xlsx', 'xls', 'xlsm']
            )
            
            if uploaded_file:
                df, error = preview_excel_file(uploaded_file)
                if error:
                    st.error(error)
                else:
                    st.success(f"Файл содержит {len(df)} строк (предпросмотр)")
                    st.dataframe(df.head(20), use_container_width=True)
                    
                    st.markdown("**Структура колонок:**")
                    col_info = pd.DataFrame({
                        'Колонка': df.columns,
                        'Тип': df.dtypes.astype(str),
                        'Уникальных значений': [df[col].nunique() for col in df.columns],
                        'Пустые значения': [df[col].isnull().sum() for col in df.columns]
                    })
                    st.dataframe(col_info, use_container_width=True)
        
        st.divider()
        
        # Кнопка очистки
        if st.button("🗑️ Очистить все данные", use_container_width=True):
            conn.execute("DROP TABLE IF EXISTS fact_sales")
            conn.execute("DROP TABLE IF EXISTS load_history")
            st.success("Данные очищены")
            st.rerun()
        
        # История загрузок
        with st.expander("📜 История загрузок"):
            history_df = get_load_history(conn)
            if not history_df.empty:
                st.dataframe(history_df, use_container_width=True)
    
    # ============================================
    # 7. ОСНОВНАЯ ОБЛАСТЬ - СВОДНАЯ ТАБЛИЦА
    # ============================================
    
    # Проверяем наличие данных
    try:
        row_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        has_data = row_count > 0
    except:
        has_data = False
        st.info("ℹ️ Нет загруженных данных. Используйте боковую панель для загрузки Excel-файлов.")
    
    if has_data:
        # Показываем информацию о данных
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Всего строк", f"{row_count:,}")
        with col2:
            columns_info = get_column_info(conn)
            dim_count = len([c for c in columns_info if not c['is_numeric']])
            metric_count = len([c for c in columns_info if c['is_numeric']])
            st.metric("📐 Измерений / 📈 Метрик", f"{dim_count} / {metric_count}")
        with col3:
            st.metric("💾 База данных", "olap_cube.duckdb")
        
        st.divider()
        
        # Интерфейс конструктора сводной таблицы
        rows, columns, values, agg_func = render_pivot_builder(conn)
        
        st.divider()
        
        # Кнопка выполнения анализа
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            analyze_button = st.button("🔄 Обновить анализ", type="primary", use_container_width=True)
        
        # Выполняем анализ
        if analyze_button or (rows or columns or values):
            if values:
                with st.spinner("🔄 Выполняется анализ данных..."):
                    result_df, message = run_pivot_query(conn, rows, columns, values, agg_func)
                    
                    if result_df is not None:
                        st.success(message)
                        
                        # Показываем результат
                        st.subheader("📊 Результат сводной таблицы")
                        
                        # Настройки отображения
                        show_options = st.checkbox("⚙️ Настройки отображения")
                        if show_options:
                            format_type = st.selectbox("Формат отображения", ["Таблица", "Транспонированная", "Тепловая карта"])
                            precision = st.slider("Точность чисел", 0, 4, 2)
                        else:
                            format_type = "Таблица"
                            precision = 2
                        
                        # Отображаем результат
                        if format_type == "Таблица":
                            styled_df = result_df.style.format(precision=precision) if precision > 0 else result_df
                            st.dataframe(styled_df, use_container_width=True, height=500)
                        
                        elif format_type == "Транспонированная":
                            st.dataframe(result_df.T, use_container_width=True, height=500)
                        
                        elif format_type == "Тепловая карта":
                            # Выбираем только числовые колонки для тепловой карты
                            numeric_cols = result_df.select_dtypes(include=['number']).columns
                            if len(numeric_cols) > 0:
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
                                st.warning("Нет числовых данных для тепловой карты")
                        
                        # Визуализация
                        if len(values) > 0 and len(result_df) > 0:
                            st.subheader("📈 Визуализация")
                            
                            viz_type = st.selectbox(
                                "Тип графика",
                                ["Столбчатая диаграмма", "Линейный график", "Круговая диаграмма", "Ящик с усами"]
                            )
                            
                            # Выбираем первую метрику для визуализации
                            metric_to_plot = values[0]
                            
                            if viz_type == "Столбчатая диаграмма":
                                # Берём первые 20 строк для читаемости
                                plot_df = result_df.head(20).reset_index()
                                x_col = plot_df.columns[0]  # Первая колонка как ось X
                                
                                fig = px.bar(
                                    plot_df,
                                    x=x_col,
                                    y=metric_to_plot,
                                    title=f"{metric_to_plot} по {x_col}",
                                    color_discrete_sequence=['#FF4B4B']
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            elif viz_type == "Линейный график":
                                plot_df = result_df.head(50).reset_index()
                                x_col = plot_df.columns[0]
                                fig = px.line(
                                    plot_df,
                                    x=x_col,
                                    y=metric_to_plot,
                                    title=f"Тренд {metric_to_plot}",
                                    markers=True
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            elif viz_type == "Круговая диаграмма":
                                plot_df = result_df.head(10).reset_index()
                                x_col = plot_df.columns[0]
                                fig = px.pie(
                                    plot_df,
                                    names=x_col,
                                    values=metric_to_plot,
                                    title=f"Распределение {metric_to_plot}"
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            
                            elif viz_type == "Ящик с усами":
                                fig = px.box(
                                    result_df.reset_index(),
                                    y=metric_to_plot,
                                    title=f"Распределение {metric_to_plot}"
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        
                        # Экспорт
                        csv = result_df.to_csv()
                        st.download_button(
                            label="📥 Скачать результаты (CSV)",
                            data=csv,
                            file_name=f"pivot_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.error(message)
            else:
                st.info("💡 Выберите хотя бы одно поле для анализа (Значения)")

if __name__ == "__main__":
    main()
