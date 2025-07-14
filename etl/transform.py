def filter_asean(df: pd.DataFrame) -> pd.DataFrame:
    """Filters for ASEAN countries and essential columns."""
    asean_countries = [
        "Brunei", "Cambodia", "Indonesia", "Laos", 
        "Malaysia", "Myanmar", "Philippines", "Singapore", 
        "Thailand", "Vietnam"
    ]
    
    # Select key columns (adjust based on your CSV structure)
    cols_to_keep = ['country', 'date', 'commodity', 'price', 'currency', 'unit']
    df = df[df['country'].isin(asean_countries)][cols_to_keep]
    
    # Convert date column (if needed)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df.dropna()