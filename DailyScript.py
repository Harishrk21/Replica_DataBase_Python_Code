import mysql.connector
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Configure logging
log_file = 'daily_sync.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# Database configurations for production and replica databases
prod_db_config = {
    'user': os.getenv('PROD_DB_USER'),
    'password': os.getenv('PROD_DB_PASSWORD'),
    'host': os.getenv('PROD_DB_HOST'),
    'database': os.getenv('PROD_DB_NAME')
}

replica_db_config = {
    'user': os.getenv('REPLICA_DB_USER'),
    'password': os.getenv('REPLICA_DB_PASSWORD'),
    'host': os.getenv('REPLICA_DB_HOST'),
    'database': os.getenv('REPLICA_DB_NAME')
}

# Batch size for processing data
BATCH_SIZE = int(os.getenv('BATCH_SIZE'))

# Fetch data older than 30 days in batches
def fetch_data_in_batches(table_name, date_column, offset):
    """
    Fetches a batch of rows older than 30 days.
    Args:
        table_name: The table to query.
        date_column: The column containing date values.
        offset: Offset for pagination.
    Returns:
        A tuple of column names and rows fetched.
    """
    cutoff_date = datetime.now() - timedelta(days=30)
    query = f"""
    SELECT * FROM {table_name}
    WHERE {date_column} < %s
    LIMIT {BATCH_SIZE} OFFSET %s
    """
    prod_conn = mysql.connector.connect(**prod_db_config)
    prod_cursor = prod_conn.cursor()
    prod_cursor.execute(query, (cutoff_date, offset))
    rows = prod_cursor.fetchall()
    columns = [col[0] for col in prod_cursor.description]
    prod_cursor.close()
    prod_conn.close()
    logging.info(f"Fetched {len(rows)} rows from {table_name} (offset {offset}).")
    return columns, rows

# Copy fetched data to the replica database
def copy_to_replica(table_name, columns, rows):
    """
    Copies data to the replica database in batches.
    """
    replica_conn = mysql.connector.connect(**replica_db_config)
    replica_cursor = replica_conn.cursor()
    column_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    insert_query = f"""
    INSERT INTO {table_name} ({column_names})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in columns])}
    """
    duplicate_count = 0  # To count duplicate entries

    try:
        for row in rows:
            try:
                replica_cursor.execute(insert_query, row)
                if replica_cursor.rowcount == 0:  # No row affected implies duplicate
                    duplicate_count += 1
            except mysql.connector.Error as err:
                logging.error(f"Error inserting row {row} into {table_name}: {err}")
        
        replica_conn.commit()
        logging.info(f"Inserted {len(rows) - duplicate_count} new rows and encountered {duplicate_count} duplicate entries in {table_name}.")
    except mysql.connector.Error as err:
        logging.error(f"Error copying data to replica for {table_name}: {err}")
    finally:
        replica_cursor.close()
        replica_conn.close()

# Delete data older than 30 days from the production database
def delete_old_data_from_primary(table_name, date_column):
    """
    Deletes data older than 30 days from the production database.
    """
    cutoff_date = datetime.now() - timedelta(days=30)
    query = f"""
    DELETE FROM {table_name}
    WHERE {date_column} < %s
    LIMIT {BATCH_SIZE}
    """
    prod_conn = mysql.connector.connect(**prod_db_config)
    prod_cursor = prod_conn.cursor()
    try:
        total_deleted = 0
        while True:
            prod_cursor.execute(query, (cutoff_date,))
            rows_affected = prod_cursor.rowcount
            if rows_affected == 0:
                break
            total_deleted += rows_affected
            prod_conn.commit()
            logging.info(f"Deleted {rows_affected} rows from {table_name}.")
        logging.info(f"Total rows deleted from {table_name}: {total_deleted}.")
    except mysql.connector.Error as err:
        logging.error(f"Error deleting data from primary database for {table_name}: {err}")
    finally:
        prod_cursor.close()
        prod_conn.close()

# Sync data older than 30 days for specific tables
def sync_old_data(table_name, date_column):
    """
    Syncs data older than 30 days for the given table.
    Copies it to the replica database and deletes it from production.
    """
    offset = 0
    while True:
        columns, rows = fetch_data_in_batches(table_name, date_column, offset)
        if not rows:
            break
        copy_to_replica(table_name, columns, rows)
        offset += BATCH_SIZE
    delete_old_data_from_primary(table_name, date_column)

# Main function to handle daily synchronization
def daily_sync():
    """
    Handles the synchronization process:
    1. Syncs data older than 30 days for specific tables.
    2. Logs progress.
    """
    try:
        # Sync appointments table
        sync_old_data("appointments", "Appointment_Date")
        
        # Sync poc_available_slots table
        sync_old_data("poc_available_slots", "Schedule_Date")
        
    except mysql.connector.Error as err:
        logging.error(f"Error during daily sync: {err}")


# Synchronize all other tables fully without deletions
def sync_full_tables():
    """
    Synchronizes all other tables completely in batches.
    Copies all rows to the replica database without removing them from production.
    """
    tables = ["Client", "POC", "Menu", "POC_Schedules", "Users", "List"]

    try:
        for table in tables:
            offset = 0
            while True:
                prod_conn = mysql.connector.connect(**prod_db_config)
                prod_cursor = prod_conn.cursor()
                query = f"SELECT * FROM {table} LIMIT {BATCH_SIZE} OFFSET %s"
                prod_cursor.execute(query, (offset,))
                rows = prod_cursor.fetchall()
                if not rows:
                    break  # Exit the loop if no rows are fetched
                columns = [col[0] for col in prod_cursor.description]
                prod_cursor.close()
                prod_conn.close()

                # Sync data without deleting from production
                copy_to_replica(table, columns, rows)
                logging.info(f"Synced {len(rows)} rows from {table} (offset {offset}).")
                offset += BATCH_SIZE  # Increment the offset for the next batch

            logging.info(f"{table} table full sync completed successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error syncing full table {table}: {err}")


# Main function to handle synchronization
def main_sync():
    """
    Handles the synchronization process:
    1. Syncs 30 days before data for specific tables (poc_available_slots & appointments).
    2. Syncs all data for other tables completely.
    """
    daily_sync()  # Run the daily sync process
    sync_full_tables()
    logging.info("Daily data sync completed successfully.")

# Run the sync process
if __name__ == "__main__":
    main_sync()
