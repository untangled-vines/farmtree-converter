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

def load_csv_to_db(df):
    conn = get_connection()
    cur = conn.cursor()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    # Convert all columns to string first to avoid float issues
    df = df.astype(str)
    # Replace 'nan' strings with None
    df = df.replace('nan', None)
    # Fix float integers e.g. 2025.0 -> 2025
    for col in df.columns:
        if df[col] is not None:
            df[col] = df[col].apply(lambda x: x.split('.')[0] if x and '.' in str(x) and str(x).split('.')[1] == '0' else x)

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
    df = pd.read_sql(f"SELECT * FROM {DB_SCHEMA}.prodai_transformed", conn, dtype=str)
    conn.close()
    # Replace None and nan with empty string
    df = df.fillna('')
    df = df.replace('None', '')
    df = df.replace('nan', '')
    # Fix float integers e.g. 136.0 -> 136
    df = df.apply(lambda col: col.map(lambda x: x.split('.')[0] if x and '.' in x and x.split('.')[1] == '0' else x))
    return df
def df_to_csv(df):
    output = io.StringIO()
    df.to_csv(output, index=False, quoting=0)  # quoting=0 = QUOTE_MINIMAL
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
                # Auto-detect delimiter
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