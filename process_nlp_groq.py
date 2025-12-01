import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from google.cloud import bigquery
from groq import Groq
import json
import sys
import time
import os

 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"path"

DB_USER = "your_username_here"
DB_PASS = os.getenv('VARIABLE_NAME')
DB_HOST = "your_host_here"
DB_PORT = "your_port_here"
DB_NAME = "your_db_name_here"

GROQ_API_KEY = os.getenv('VARIABLE_NAME')

if not DB_PASS or not GROQ_API_KEY:
    print("ERROR: DB_PASS or GROQ_API_KEY environment variables not set.")
    sys.exit(1)

BIGQUERY_TABLE_ID = "your_bq_table_id"
POSTGRES_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# --- 2. SETUP API CLIENTS ---
try:
    pg_engine = create_engine(POSTGRES_URL)
    bq_client = bigquery.Client()
    groq_client = Groq(api_key=GROQ_API_KEY)
except Exception as e:
    print(f"Error setting up clients: {e}", file=sys.stderr)
    sys.exit(1)

# --- 3. HELPER FUNCTIONS ---

def get_groq_response_single(message_text, auto_id):
    """Processes a SINGLE pre-filtered message."""
    system_prompt = """
    You are an expert financial SMS parser. You will receive a single SMS message
    that has been pre-filtered and is a financial transaction.
    
    You MUST respond with a single, valid JSON object.
    
    For the SMS, extract:
    1.  "is_transaction": true
    2.  "vendor_name": (string, or "null")
    3.  "amount": (float, or "null")
    4.  "transaction_type": ("debit", "credit", or "info")
    5.  "inferred_category": (One of: "Food", "Subscription", "Utilities", "Bank Alert", "Transport", "Shopping", "Other")
    """
    user_prompt_content = f"Here is the SMS message to process:\n{message_text}"

    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt_content}],
            model="llama-3.1-8b-instant",
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        response_text = chat_completion.choices[0].message.content
        response_data = json.loads(response_text)
        
        if isinstance(response_data, dict):
            if "results" in response_data and isinstance(response_data["results"], list) and len(response_data["results"]) > 0:
                return response_data["results"][0]
            elif "is_transaction" in response_data:
                return response_data
        
        raise ValueError("Single response was not a valid JSON object.")

    except (json.JSONDecodeError, ValueError) as e:
        print(f"  POISON PILL: ID {auto_id} failed. Reason: {e}.", file=sys.stderr)
        return None # Mark as "error"
    # Any other error (like 429) will stop the script

def load_dataframe_to_bigquery(df):
    if df.empty:
        print("No valid rows to load into BigQuery.")
        return
    print(f"Loading {len(df)} processed rows into BigQuery...")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("message_auto_id", "INT64"),
            bigquery.SchemaField("timestamp_utc", "TIMESTAMP"),
            bigquery.SchemaField("sender_address", "STRING"),
            bigquery.SchemaField("message_body", "STRING"),
            bigquery.SchemaField("is_transaction", "BOOL"),
            bigquery.SchemaField("vendor_name", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("currency", "STRING"), 
            bigquery.SchemaField("transaction_type", "STRING"),
            bigquery.SchemaField("inferred_category", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )
    job = bq_client.load_table_from_dataframe(
        df, BIGQUERY_TABLE_ID, job_config=job_config
    )
    job.result()
    print(f"Successfully loaded {len(df)} rows to {BIGQUERY_TABLE_ID}.")

def update_postgres_status(message_ids, status="processed"):
    if not message_ids:
        return
    print(f"Updating {len(message_ids)} rows in Postgres to '{status}'...")
    id_tuple = tuple(message_ids)
    sql_safe_ids = ""
    if len(id_tuple) == 1:
        sql_safe_ids = f"({id_tuple[0]})"
    else:
        sql_safe_ids = str(id_tuple)     
    with pg_engine.connect() as conn:
        conn.execute(
            text(f"UPDATE raw_notifications SET processing_status = '{status}' WHERE auto_id IN {sql_safe_ids}")
        )
        conn.commit() 

# --- 4. MAIN ORCHESTRATION (THE EFFICIENT ROW-BY-ROW LOGIC) ---
def main():
    print("--- Starting NLP Enrichment Script (Row-by-Row) ---")
    
    # We will fetch 20 rows at a time.
    # 20 rows/batch * 3 sec/req = 60 seconds.
    # This perfectly matches our 20 RPM Groq limit.
    BATCH_SIZE = 20 
    
    try:
        while True:
            # 1. Fetch ONLY pre-filtered rows
            df = pd.read_sql(
                f"SELECT auto_id, timestamp_utc, sender_address, message_body FROM raw_notifications WHERE processing_status = 'pre_filtered' ORDER BY auto_id ASC LIMIT {BATCH_SIZE}",
                pg_engine
            )
            
            if df.empty:
                print("\nNo more 'pre_filtered' rows found. Pipeline complete.")
                break 

            print(f"\n--- Processing Batch (Rows {df['auto_id'].min()}-{df['auto_id'].max()}) ---")
            
            processed_data_list = []
            processed_ids = []
            error_ids = []
            
            # 2. Loop row-by-row (this is reliable)
            for index, row in df.iterrows():
                auto_id = row['auto_id']
                message_text = row['message_body']
                
                print(f"    Processing ID: {auto_id}...", end="")
                
                single_result = get_groq_response_single(message_text, auto_id)
                
                if single_result:
                    print(" OK")
                    single_result.update({
                        'message_auto_id': auto_id,
                        'timestamp_utc': row['timestamp_utc'],
                        'sender_address': row['sender_address'],
                        'message_body': message_text
                    })
                    processed_data_list.append(single_result)
                    processed_ids.append(auto_id)
                else:
                    print(" FAILED (Poison Pill)")
                    error_ids.append(auto_id)
                
                # --- THIS IS THE 20 RPM RATE LIMIT FIX ---
                time.sleep(3) # 60 sec / 20 req = 3 sec/req
            
            # 3. Load & Update
            if processed_data_list:
                final_df = pd.DataFrame(processed_data_list)

                print("  Cleaning data types (str 'null' -> pd.NA)...")
                final_df['amount'] = pd.to_numeric(final_df['amount'], errors='coerce')

                final_df['currency'] = 'INR'
                final_df.rename(columns={'auto_id': 'message_auto_id'}, inplace=True)
                bq_columns = [
                    'message_auto_id', 'timestamp_utc', 'sender_address', 'message_body',
                    'is_transaction', 'vendor_name', 'amount', 'currency',
                    'transaction_type', 'inferred_category'
                ]
                final_df = final_df.reindex(columns=bq_columns)
                load_dataframe_to_bigquery(final_df)
            
            update_postgres_status(processed_ids, status="processed")
            update_postgres_status(error_ids, status="error")
                
            print(f"--- Batch complete. Good: {len(processed_ids)}, Poison Pills: {len(error_ids)} ---")

    except Exception as e:
        print(f"A CRITICAL, unrecoverable error occurred: {e}", file=sys.stderr)
        print("Stopping the pipeline to prevent data corruption.")
        sys.exit(1)

    print("\n--- NLP Enrichment Run Complete ---")


if __name__ == "__main__":
    main()