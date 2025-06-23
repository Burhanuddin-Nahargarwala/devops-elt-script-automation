import pandas as pd

# --- 2. Transformation ---
def transform_data(raw_data: list) -> pd.DataFrame:
    """
    Cleans and transforms the raw JSON data.
    This function uses Pandas and can be unit-tested locally without Spark.
    """
    print("Transforming data...")
    # Use pandas to flatten the nested JSON structure. 'address' and 'company' are nested.
    df = pd.json_normalize(raw_data, sep='_')
    
    # Select and rename columns for clarity
    df = df[['id', 'name', 'username', 'email', 'address_city', 'company_name']]
    df = df.rename(columns={
        'address_city': 'city',
        'company_name': 'company'
    })
    print("Data transformed successfully.")
    return df