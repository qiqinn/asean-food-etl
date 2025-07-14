import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

def load(df) -> duckdb.DuckDBPyConnection:
    """Saves data to DuckDB and Parquet."""
    # Save to Parquet (for Spark/Streamlit)
    df.to_parquet(os.getenv("PROCESSED_DATA_PATH"))
    
    # Load into DuckDB
    conn = duckdb.connect(":memory:")
    conn.register("asean_food", df)
    
    # Example: Create a summary view
    conn.execute("""
        CREATE VIEW summary_stats AS
        SELECT 
            country, 
            COUNT(*) AS records,
            AVG(price) AS avg_price
        FROM asean_food
        GROUP BY country
    """)
    return conn