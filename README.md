# Daily Data Synchronization Script

This Python script is designed to synchronize data daily between a production database and a replica database. It helps ensure data consistency while managing older data efficiently.  

The script processes specific tables incrementally, handles duplicates, and synchronizes others fully without deletion. It logs detailed information for monitoring and troubleshooting.

---

## Features

- Synchronizes data older than 30 days for specific tables (`appointments` and `poc_available_slots`) and removes it from the production database.
- Performs full synchronization for other tables without deletions.
- Supports batch processing for efficient data handling (configurable via `BATCH_SIZE`).
- Gracefully handles duplicate entries with detailed logging.
- Ensures all operations are logged to a file for monitoring.

---

