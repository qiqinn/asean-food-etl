import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

def extract() -> pd.DataFrame:
    """Loads the WFP food prices CSV."""
    filepath = os.getenv("RAW_DATA_PATH")
    return pd.read_csv(filepath, encoding='utf-8')  # or 'latin1' if UTF-8 fails