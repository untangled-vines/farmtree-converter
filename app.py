import streamlit as st
import pandas as pd
import psycopg2
import io

DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_NAME = st.secrets["DB_NAME"]
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_SCHEMA = st.secrets["DB_SCHEMA"]

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def strip_dot_zero(x):
    """Strip trailing .0 from integer-like strings, leave real decimals (e.g. 0.5) intact."""
    s = str(x) if x is not None else ''
    if '.' in s:
        parts = s.split('.')
        if parts[1] == '0':
            return parts[0]
    return s

def load_csv_to_db(df):
    conn = get_connection()
    cur = conn.cursor()
    df = df.copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    df = df.astype(str)
    df = df.replace('nan', None)

    # Strip .0 only from cells that are truly integer-like (e.g. "1.0" → "1")
    for col in df.columns:
        df[col] = df[col].apply(lambda x: strip_dot_zero(x) if x is not None else x)

    cur.execute(f"TRUNCATE {DB_SCHEMA}.prodai")
    cols = ','.join([f'"{c}"' for c in df.columns])
    placeholders = ','.join(['%s'] * len(df.columns))
    for _, row in df.iterrows():
        values = [None if v == 'nan' or v is None else v for v in row]
        cur.execute(f"INSERT INTO {DB_SCHEMA}.prodai ({cols}) VALUES ({placeholders})", values)
    conn.commit()
    cur.close()
    conn.close()

def get_transformed_data():
    conn = get_connection()
    # Use a cursor + fetchall instead of pd.read_sql to avoid the psycopg2 UserWarning
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {DB_SCHEMA}.prodai_transformed")
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    df = pd.DataFrame(rows, columns=cols).astype(str)
    df = df.replace({'None': '', 'nan': '', '<NA>': ''})

    # Safely strip .0 from integer-like values, preserve real decimals like 0.5
    df = df.apply(lambda col: col.map(strip_dot_zero))

    return df

def df_to_csv(df):
    output = io.StringIO()
    df.to_csv(output, index=False, quoting=0)
    return output.getvalue().encode('utf-8')

# --- UI ---
st.title("🌱 Acorn → FarmTree Converter")
st.write("Upload your Acorn CSV export to convert it to FarmTree multiplot format.")

uploaded_file = st.file_uploader("Upload Acorn CSV", type="csv")
if uploaded_file:
    st.info("File uploaded — click Convert to process it.")
    if st.button("Convert"):
        with st.spinner("Loading data..."):
            try:
                sample = uploaded_file.read(2048).decode('utf-8')
                uploaded_file.seek(0)
                delimiter = ';' if sample.count(';') > sample.count(',') else ','
                df_input = pd.read_csv(uploaded_file, delimiter=delimiter, encoding='utf-8')
                st.success(f"Loaded {len(df_input)} farmer records")
            except Exception as e:
                st.error(f"Failed to read CSV: {e}")
                st.stop()

        with st.spinner("Transforming data..."):
            try:
                load_csv_to_db(df_input)
                df_output = get_transformed_data()
                st.success(f"Transformed {len(df_output)} plots successfully!")
            except Exception as e:
                st.error(f"Transformation failed: {e}")
                st.stop()

        csv_bytes = df_to_csv(df_output)
        st.download_button(
            label="⬇️ Download FarmTree CSV",
            data=csv_bytes,
            file_name="farmtree_export.csv",
            mime="text/csv"
        )
