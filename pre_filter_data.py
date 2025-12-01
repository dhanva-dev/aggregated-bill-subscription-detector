import pandas as pd
from sqlalchemy import create_engine, text
import os
import re
import sys

# --- 1. SET CREDENTIALS ---
DB_USER = "your_username_here"
DB_PASS = os.getenv('VARIABLE_NAME')
DB_HOST = "your_host_here"
DB_PORT = "your_port_here"
DB_NAME = "your_db_name_here"

if not DB_PASS:
    print("ERROR: DB_PASS environment variable not set.")
    sys.exit(1)

POSTGRES_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
pg_engine = create_engine(POSTGRES_URL)

# --- 2. DEFINE FILTERS ---
TRANSACTION_KEYWORDS_REGEX = re.compile(
    r'\b(rs\.?|inr|paid|received|credited|debited|a/c|transaction|payment|charged|sent to|received from|spent|recharge of)\b',
    re.IGNORECASE
)
OTP_KEYWORDS_REGEX = re.compile(
    r'\b(otp|code|verification|verify)\b|G-\d{6}',
    re.IGNORECASE
)

def pre_filter_message(message_body):
    """Uses Regex to classify a message. Returns: 'pre_filtered', 'junk'"""
    if pd.isna(message_body) or len(message_body) < 15:
        return 'junk'
    
    # Check for non-English chars
    if not all(ord(c) < 128 for c in message_body):
        return 'junk'
        
    if TRANSACTION_KEYWORDS_REGEX.search(message_body):
        return 'pre_filtered' # This is a potential transaction
        
    if OTP_KEYWORDS_REGEX.search(message_body):
        return 'junk'
    
    # Everything else is junk
    return 'junk'

def update_status_in_db(id_list, status):
    """Helper function to update statuses in bulk"""
    if not id_list:
        return
    
    print(f"  Updating {len(id_list)} rows to '{status}'...")
    id_tuple = tuple(id_list)
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

# --- 3. MAIN FILTERING LOOP ---
def main():
    print("--- Starting Pre-filtering Script ---")
    BATCH_SIZE = 10000 # Process 10,000 rows at a time
    total_good = 0
    total_junk = 0
    
    while True:
        print(f"\nFetching next {BATCH_SIZE} rows...")
        df = pd.read_sql(
            f"SELECT auto_id, message_body FROM raw_notifications WHERE processing_status = 'pending' ORDER BY auto_id ASC LIMIT {BATCH_SIZE}",
            pg_engine
        )
        
        if df.empty:
            print("No more pending rows found.")
            break
            
        # Apply the fast, local filter
        df['filter_status'] = df['message_body'].apply(pre_filter_message)
        
        good_rows = df[df['filter_status'] == 'pre_filtered']['auto_id'].tolist()
        junk_rows = df[df['filter_status'] == 'junk']['auto_id'].tolist()
        
        # Update the database
        update_status_in_db(good_rows, 'pre_filtered')
        update_status_in_db(junk_rows, 'junk')
        
        total_good += len(good_rows)
        total_junk += len(junk_rows)
        
    print("\n--- Pre-filtering Complete ---")
    print(f"Total Transactions: {total_good}")
    print(f"Total Junk Rows:    {total_junk}")
    print("You can now run 'process_nlp_groq.py' to enrich the transactions.")

if __name__ == "__main__":
    main()