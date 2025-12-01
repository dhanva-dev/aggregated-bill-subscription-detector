import os
import pandas as pd
from google.cloud import bigquery
from scipy import stats
import sys


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path"

# Set your project and table IDs
BQ_PROJECT_ID = "your_project_id_here"
BQ_DATASET_ID = "your_database_id_here"
SOURCE_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.agg_monthly_spend"
DESTINATION_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.fct_anomalies"


ANOMALY_THRESHOLD = 2.5 

try:
    bq_client = bigquery.Client()
    print("BigQuery client created successfully.")
except Exception as e:
    print(f" CRITICAL: Could not create BigQuery client. Check credentials. Error: {e}")
    sys.exit(1)

# --- 2. FUNCTION TO CALCULATE Z-SCORE SAFELY ---
def calculate_safe_zscore(group):
    """
    Calculates Z-score for a group.
    Handles groups with too few data points to be statistically valid.
    """
    # A Z-score is meaningless with 2 or fewer data points
    if len(group) < 3:
        group['z_score'] = 0.0 # Assign a neutral score
    else:
        # Calculate Z-score for 'total_amount'
        # nan_policy='omit' will ignore nulls
        # ddof=0 calculates the population standard deviation
        group['z_score'] = stats.zscore(group['total_amount'], ddof=0, nan_policy='omit')
    
    return group

# --- 3. MAIN ANALYSIS LOGIC ---
def run_anomaly_detection():
    print(f"--- Starting Anomaly Detection ---")
    
    # 1. Fetch data from BigQuery
    print(f"Fetching data from {SOURCE_TABLE}...")
    try:
        query = f"SELECT * FROM `{SOURCE_TABLE}`"
        df = bq_client.query(query).to_dataframe()
        print(f"Fetched {len(df)} rows.")
    except Exception as e:
        print(f"CRITICAL: Could not fetch data from BigQuery. Error: {e}")
        return

    if df.empty:
        print("No data in source table. Exiting.")
        return

    # 2. Calculate Z-scores
    print("Calculating Z-scores per category...")
    # We MUST group by category. A high 'Utilities' bill should not
    # be compared to a 'Food' bill.
    # .groupby().apply() runs our function on each category subset
    df_scores = df.groupby('inferred_category').apply(calculate_safe_zscore)
    
    # Fill any NaN z_scores (from single-item groups) with 0
    df_scores['z_score'].fillna(0.0, inplace=True)

    # 3. Filter for anomalies
    # We only care about rows where the absolute Z-score is high
    anomalies_df = df_scores[abs(df_scores['z_score']) >= ANOMALY_THRESHOLD].copy()
    
    if anomalies_df.empty:
        print("No anomalies found. All spending is normal.")
        return

    print(f"Found {len(anomalies_df)} anomalies!")
    
    # 4. Prepare and upload to BigQuery
    anomalies_df['run_timestamp'] = pd.Timestamp.now(tz='UTC')
    
    # Ensure columns match the BQ schema
    final_columns = ['spend_month', 'inferred_category', 'total_amount', 'z_score', 'run_timestamp']
    anomalies_df = anomalies_df[final_columns]
    
    print(f"Uploading anomalies to {DESTINATION_TABLE}...")
    job_config = bigquery.LoadJobConfig(
        # We want to overwrite the table each time we run
        write_disposition="WRITE_TRUNCATE", 
    )
    
    try:
        job = bq_client.load_table_from_dataframe(
            anomalies_df, DESTINATION_TABLE, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        print(f"Successfully uploaded {len(anomalies_df)} anomalies to BigQuery.")
    except Exception as e:
        print(f"CRITICAL: Could not upload anomalies to BigQuery. Error: {e}")

if __name__ == "__main__":
    run_anomaly_detection()