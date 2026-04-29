import streamlit as st
import pandas as pd
import psycopg2
import io
import math

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
def strip_dot_zero(x, column_name=None):
    """Strip trailing .0 from integer-like floats, leave real decimals (e.g. 0.5) intact."""
    # Ensure we work with a string if the value is not None
    s = str(x) if x is not None else ''
    
    # Check if the value contains a decimal point
    if '.' in s:
        # Split into integer and decimal parts
        parts = s.split('.')
        
        # If the decimal part is '0' and the integer part is numeric (not negative and no other decimals), strip .0
        if parts[1] == '0' and parts[0].isdigit():
            return parts[0]  # Strip .0 from integer-like values (e.g. 5.0 -> 5)
    
    # Return the original value if it's a decimal (e.g. 0.5, 2.5, etc.)
    return s
    
# Function to safely convert a value to None if it represents a null/NaN
import math

def safe_null(v):
    """Return None for any NaN/null variant, strip .0 from integer-like floats, otherwise return value as-is."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):  # Handle NaN explicitly
            return None  # Return None instead of NaN
        # Convert float to int only if it's a whole number (e.g., 2025.0 -> 2025)
        if v == int(v):
            return int(v)  # Leave decimal values (like 0.5) as float
        return v
    if isinstance(v, str):
        if v.strip().lower() in ('nan', 'none', 'nat', ''):
            return None
        # Handle string "2025.0" -> "2025"
        if '.' in v:
            parts = v.split('.')
            if parts[1] == '0' and parts[0].lstrip('-').isdigit():
                return int(parts[0])  # Convert to int if it looks like a whole number
        return v
    return v
    
# Function to load CSV data into the database
def load_csv_to_db(df):
    conn = get_connection()
    cur = conn.cursor()

    # Clean column names for compatibility
    df = df.copy()
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Clear existing data in the target table
    cur.execute(f"TRUNCATE {DB_SCHEMA}.prodai")

    # Insert data row by row
    cols = ','.join([f'"{c}"' for c in df.columns])
    placeholders = ','.join(['%s'] * len(df.columns))

    for _, row in df.iterrows():
        # Apply strip_dot_zero and safe_null logic to each column depending on its name
        values = [
            safe_null(strip_dot_zero(v, 'year')) if 'planting_year' in col and col.startswith('plots') 
            else safe_null(strip_dot_zero(v))
            for col, v in zip(df.columns, row)
        ]

        # Execute the insert query with the processed values
        cur.execute(
            f"INSERT INTO {DB_SCHEMA}.prodai ({cols}) VALUES ({placeholders})",
            values
        )

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

    df = pd.DataFrame(rows, columns=cols)
    # Don't use astype(str) globally — convert per column carefully
    df = df.where(pd.notnull(df), None)
    
    # Convert to string but preserve numeric decimals correctly
    def safe_str(v):
        if v is None:
            return ''
        from decimal import Decimal
        if isinstance(v, Decimal):
            # Normalize: remove trailing zeros but keep meaningful decimals
            return format(v.normalize(), 'f')
        return str(v)
    
    try:
    # pandas 2.1+
    return df.map(safe_str).replace({'None': '', 'nan': '', '<NA>': ''})
except AttributeError:
    # pandas < 2.1 fallback
    return df.applymap(safe_str).replace({'None': '', 'nan': '', '<NA>': ''})

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
