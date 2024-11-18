import mysql.connector
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Configure logging to capture the sync process details
log_file = 'monthly_sync.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# File handler to log messages into a file
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

# Helper function to calculate the previous month and year
def get_previous_month_and_year():
    """
    Returns the previous month and year.
    Handles year rollover for January.
    """
    current_month = datetime.now().month
    current_year = datetime.now().year
    if current_month == 1:  # If it's January, return December of the previous year
        return 12, current_year - 1
    else:
        return current_month - 1, current_year

# Fetch data for the previous month from the production database
def fetch_previous_month_data(table_name, date_column):
    """
    Fetches rows for the previous month from the specified table.
    Args:
        table_name: The table to query.
        date_column: The column containing date values.
    Returns:
        A tuple of column names and rows fetched.
    """
    prev_month, prev_year = get_previous_month_and_year()
    query = f"""
    SELECT * FROM {table_name}
    WHERE MONTH({date_column}) = %s AND YEAR({date_column}) = %s
    """
    prod_conn = mysql.connector.connect(**prod_db_config)
    prod_cursor = prod_conn.cursor()
    prod_cursor.execute(query, (prev_month, prev_year))
    rows = prod_cursor.fetchall()
    columns = [col[0] for col in prod_cursor.description]
    prod_cursor.close()
    prod_conn.close()
    return columns, rows

# Copy fetched data to the replica database
def copy_to_replica(table_name, columns, rows):
    """
    Copies data from production to replica database, handling duplicates efficiently.
    """
    replica_conn = mysql.connector.connect(**replica_db_config)
    replica_cursor = replica_conn.cursor()
    column_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    # Use INSERT ON DUPLICATE KEY UPDATE for handling duplicates
    insert_query = f"""
    INSERT INTO {table_name} ({column_names})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in columns])}
    """
    try:
        for row in rows:
            logging.debug(f"Processing row: {row}")
            try:
                replica_cursor.execute(insert_query, row)
            except mysql.connector.Error as err:
                logging.error(f"Error inserting row {row} into {table_name}: {err}")
                continue  # Skip to the next row
        replica_conn.commit()
        logging.info(f"Data for {table_name} copied to replica successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error copying data to replica for {table_name}: {err}")
    finally:
        replica_cursor.close()
        replica_conn.close()

# Delete previous month's data from the production database
def delete_previous_month_data_from_primary(table_name, date_column):
    """
    Deletes rows for the previous month from the production database.
    Args:
        table_name: The table to delete data from.
        date_column: The column containing date values.
    """
    prev_month, prev_year = get_previous_month_and_year()
    query = f"""
    DELETE FROM {table_name}
    WHERE MONTH({date_column}) = %s AND YEAR({date_column}) = %s
    """
    try:
        prod_conn = mysql.connector.connect(**prod_db_config)
        prod_cursor = prod_conn.cursor()
        prod_cursor.execute(query, (prev_month, prev_year))
        prod_conn.commit()
        rows_affected = prod_cursor.rowcount
        prod_cursor.close()
        prod_conn.close()
        logging.info(f"Deleted {rows_affected} rows from {table_name} in the primary database.")
    except mysql.connector.Error as err:
        logging.error(f"Error deleting data from primary database for {table_name}: {err}")

# Synchronize appointments and available slots for the previous month
def sync_previous_month_tables():
    """
    Synchronizes data for 'appointments' and 'poc_available_slots' tables.
    Copies data for the previous month to the replica database
    and deletes it from the production database.
    """
    try:
        # Sync appointments table
        columns, rows = fetch_previous_month_data("appointments", "Appointment_Date")
        copy_to_replica("appointments", columns, rows)
        delete_previous_month_data_from_primary("appointments", "Appointment_Date")
        
        # Sync poc_available_slots table
        columns, rows = fetch_previous_month_data("poc_available_slots", "Schedule_Date")
        copy_to_replica("poc_available_slots", columns, rows)
        delete_previous_month_data_from_primary("poc_available_slots", "Schedule_Date")
        
        logging.info("Sync for previous month (appointments and poc_available_slots) completed successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error during sync for previous month: {err}")

# Synchronize all other tables fully without deletions
def sync_full_tables():
    """
    Synchronizes all other tables completely.
    Copies all rows to the replica database without removing them from production.
    """
    tables = ["Client", "POC", "Menu", "POC_Schedules", "Users", "List"]
    try:
        for table in tables:
            prod_conn = mysql.connector.connect(**prod_db_config)
            prod_cursor = prod_conn.cursor()
            prod_cursor.execute(f"SELECT * FROM {table}")
            rows = prod_cursor.fetchall()
            columns = [col[0] for col in prod_cursor.description]
            prod_cursor.close()
            prod_conn.close()

            # Sync data without deleting from production
            copy_to_replica(table, columns, rows)
            logging.info(f"{table} table sync completed successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error syncing full table {table}: {err}")

# Main function to handle synchronization
def main_sync():
    """
    Handles the synchronization process:
    1. Syncs data for the previous month for specific tables.
    2. Syncs all data for other tables completely.
    """
    sync_previous_month_tables()
    sync_full_tables()
    logging.info("Monthly data sync completed successfully.")

# Run the sync process
if __name__ == "__main__":
    main_sync()
