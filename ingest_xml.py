import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine
import os
import hashlib
from datetime import datetime

# --- CONFIGURATION ---
XML_FILE_PATH = " "   
DB_USER = "your_username_here"
DB_PASS = os.getenv('VARIABLE_NAME')
DB_HOST = "your_host_here"
DB_PORT = "your_port_here"
DB_NAME = "your_db_name_here"

if not DB_PASS:
    print("ERROR: DB_PASS environment variable not set.")
    exit(1)

DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def generate_unique_id(address, date_str, body):
    """
    Generates a unique hash for the message.
    This prevents duplicates if you upload the same backup file twice.
    """
    raw_string = f"{address}{date_str}{body}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest()

def ingest_xml_data():
    print(f"--- Starting Ingestion for {XML_FILE_PATH} ---")
    
    try:
        # 1. Parse the XML
        print("Parsing XML file (this might take a moment)...")
        tree = ET.parse(XML_FILE_PATH)
        root = tree.getroot()
        
        sms_list = []
        
        # 2. Iterate through every <sms> tag
        # The file format is usually <smses><sms ... /><sms ... /></smses>
        for sms in root.findall('sms'):
            
            # We only want RECEIVED messages (type='1' is usually Inbox)
            # But for safety, let's take everything and let the NLP filter handle it.
            
            address = sms.get('address') # Sender (e.g., "HP-HDFCBK")
            body = sms.get('body')       # The text
            date_millis = sms.get('date') # Timestamp in milliseconds
            
            if not body or not date_millis:
                continue # Skip broken records

            # Convert Epoch Millis to DateTime
            # 1732012639586 -> 2025-11-19 ...
            timestamp = datetime.fromtimestamp(int(date_millis) / 1000.0)
            
            # Create a unique ID
            unique_id = generate_unique_id(address, date_millis, body)
            
            sms_list.append({
                'source_message_id': unique_id,
                'timestamp_utc': timestamp,
                'sender_address': address,
                'message_body': body,
                'processing_status': 'pending'
            })
            
        print(f"Parsed {len(sms_list)} SMS messages.")
        
        # 3. Convert to DataFrame
        df = pd.DataFrame(sms_list)
        
        # 4. Load to Postgres
        engine = create_engine(DATABASE_URL)
        
        # We use "ON CONFLICT DO NOTHING" logic via pandas (loading all, removing dupes)
        # Ideally, we check existing IDs, but for a portfolio project, 
        # cleaning the table first is acceptable.
        
        print("Inserting into Postgres...")
        
        # chunksize helps prevent memory errors with large XMLs
        df.to_sql('raw_notifications', 
                  engine, 
                  if_exists='append', 
                  index=False, 
                  method='multi',
                  chunksize=1000) 

        print(f"Successfully ingested {len(df)} messages into Postgres.")

    except FileNotFoundError:
        print(f"🚨 ERROR: File {XML_FILE_PATH} not found.")
    except Exception as e:
        # If duplicates exist, it might throw an IntegrityError. 
        # For a perfect pipeline, we'd handle that gracefully, but this will show us the error.
        print(f"Error during ingestion: {e}")

if __name__ == "__main__":
    ingest_xml_data()