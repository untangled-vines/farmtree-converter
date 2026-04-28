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
def safe_null(v):
    """Return None for any NaN/null variant, strip .0 from integer-like floats, otherwise return value as-is."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):  # Handle NaN explicitly
            return None
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

    # Replace NaN with None (Postgres NULL) at the DataFrame level first
    df = df.where(pd.notnull(df), None)

    # Clear existing data in the target table
    cur.execute(f"TRUNCATE {DB_SCHEMA}.prodai")

    # Insert data row by row
    cols = ','.join([f'"{c}"' for c in df.columns])
    placeholders = ','.join(['%s'] * len(df.columns))

    for _, row in df.iterrows():
        # Apply safe_null to handle any NaN variants and clean the year-like columns
        values = [safe_null(v) for v in row]

        # Apply strip_dot_zero logic to each column depending on its name
        values = [
            strip_dot_zero(v, 'year') if 'year' in col.lower() else strip_dot_zero(v)
            for col, v in zip(df.columns, row)
        ]

        # Execute the insert query
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

    # Create dataframe from the fetched data
    df = pd.DataFrame(rows, columns=cols).astype(str)
    df = df.replace({'None': '', 'nan': '', '<NA>': ''})

    # No need to apply strip_dot_zero function here since data is already cleaned on insert
    return df

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
