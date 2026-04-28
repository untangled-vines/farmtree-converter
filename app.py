import streamlit as st
import pandas as pd
import psycopg2
import io

# DB connection details from Streamlit secrets
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_NAME = st.secrets["DB_NAME"]
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_SCHEMA = st.secrets["DB_SCHEMA"]

# Function to get the database connection
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Function to clean integer-like string values (strip .0)
def strip_dot_zero(x):
    """Strip trailing .0 from integer-like strings, leave real decimals (e.g. 0.5) intact."""
    s = str(x) if x is not None else ''
    if '.' in s:
        parts = s.split('.')
        if parts[1] == '0':
            return parts[0]
    return s

# Function to load CSV data into the database
def load_csv_to_db(df):
    conn = get_connection()
    cur = conn.cursor()

    # Clean column names for compatibility
    df = df.copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Replace NaN with None (Postgres NULL)
    df = df.where(pd.notnull(df), None)

    # Clear existing data in the target table
    cur.execute(f"TRUNCATE {DB_SCHEMA}.prodai")
    
    # Insert data row by row
    cols = ','.join([f'"{c}"' for c in df.columns])
    placeholders = ','.join(['%s'] * len(df.columns))
    for _, row in df.iterrows():
        values = [None if v == 'nan' or v is None else v for v in row]
        cur.execute(f"INSERT INTO {DB_SCHEMA}.prodai ({cols}) VALUES ({placeholders})", values)
    
    conn.commit()
    cur.close()
    conn.close()

# Function to fetch transformed data from the database
def get_transformed_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {DB_SCHEMA}.prodai_transformed")
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    # Create dataframe and clean-up
    df = pd.DataFrame(rows, columns=cols).astype(str)
    df = df.replace({'None': '', 'nan': '', '<NA>': ''})

    # Safely strip .0 from integer-like values, preserve real decimals like 0.5
    df = df.apply(lambda col: col.map(strip_dot_zero))

    return df

# Function to convert dataframe to CSV format
def df_to_csv(df):
    output = io.StringIO()
    df.to_csv(output, index=False, quoting=0)
    return output.getvalue().encode('utf-8')

# Main app function
def main():
    st.title("🌱 Acorn → FarmTree Converter")
    st.write("Upload your Acorn CSV export to convert it to FarmTree multiplot format.")

    # Upload CSV file
    uploaded_file = st.file_uploader(
        "Upload Acorn CSV",
        type="csv",
        key="acorn_csv_uploader"
    )

    if uploaded_file:
        st.info("File uploaded — click Convert to process it.")
        
        # Button to trigger conversion process
        if st.button("Convert", key="convert_btn"):
            with st.spinner("Loading data..."):
                try:
                    # Read and determine delimiter based on sample content
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
                    # Load the data into the DB and get the transformed output
                    load_csv_to_db(df_input)
                    df_output = get_transformed_data()
                    st.success(f"Transformed {len(df_output)} plots successfully!")
                except Exception as e:
                    st.error(f"Transformation failed: {e}")
                    st.stop()

            # Convert dataframe to CSV and provide download button
            csv_bytes = df_to_csv(df_output)
            st.download_button(
                label="⬇️ Download FarmTree CSV",
                data=csv_bytes,
                file_name="farmtree_export.csv",
                mime="text/csv"
            )

# Run the app
if __name__ == "__main__":
    main()
