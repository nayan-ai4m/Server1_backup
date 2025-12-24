import psycopg2
import time
from datetime import datetime
import threading

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'short_data_hul'
DB_USER = 'postgres'
DB_PASSWORD = 'ai4m2024'

# Tables to cleanup (all 6 short_data tables)
TABLES = ['mc25_short_data', 'mc26_short_data', 'mc27_short_data',
          'mc28_short_data', 'mc29_short_data', 'mc30_short_data']

# Keep most recent 100 rows per table
KEEP_ROWS = 100

class DatabaseCleaner:
    def __init__(self):
        self.db_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        self.running = True

    def delete_old_records(self, table_name):
        """Delete old records from a specific table, keeping newest KEEP_ROWS"""
        conn = None
        cursor = None

        try:
            # Establish connection
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            # More efficient approach: Use row_number() window function
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE timestamp < (
                    SELECT timestamp
                    FROM {table_name}
                    ORDER BY timestamp DESC
                    OFFSET {KEEP_ROWS} LIMIT 1
                )
            """

            # Execute the DELETE command
            cursor.execute(delete_query)
            deleted_rows = cursor.rowcount

            # Commit the transaction
            conn.commit()

            if deleted_rows > 0:
                print(f"{datetime.now()}: Deleted {deleted_rows} old rows from {table_name}")

            return deleted_rows

        except (Exception, psycopg2.Error) as error:
            print(f"{datetime.now()}: Error deleting rows from {table_name}: {error}")
            return 0

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def cleanup_all_tables(self):
        """Clean up all tables"""
        total_deleted = 0

        for table in TABLES:
            if not self.running:
                break
            deleted = self.delete_old_records(table)
            total_deleted += deleted

        if total_deleted > 0:
            print(f"{datetime.now()}: Total cleanup completed - {total_deleted} rows deleted across all tables")

        return total_deleted

    def run_periodic_cleanup(self, interval_hours=1):
        """Run cleanup periodically"""
        print(f"{datetime.now()}: Starting periodic cleanup every {interval_hours} hour(s)")

        try:
            while self.running:
                self.cleanup_all_tables()

                # Sleep for the specified interval (in seconds)
                time.sleep(interval_hours * 60 * 60)

        except KeyboardInterrupt:
            print(f"\n{datetime.now()}: Cleanup stopped by user")
        finally:
            self.running = False

    def stop(self):
        """Stop the cleanup process"""
        self.running = False

# Alternative approach: More efficient batch cleanup
class OptimizedDatabaseCleaner:
    def __init__(self):
        self.db_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        self.running = True

    def cleanup_all_tables_batch(self):
        """Clean up all tables in a single connection"""
        conn = None
        cursor = None
        total_deleted = 0

        try:
            # Use single connection for all operations
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            for table in TABLES:
                if not self.running:
                    break

                # Check if table has more than KEEP_ROWS records
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]

                if row_count > KEEP_ROWS:
                    # Delete excess rows
                    delete_query = f"""
                        DELETE FROM {table}
                        WHERE timestamp < (
                            SELECT timestamp
                            FROM {table}
                            ORDER BY timestamp DESC
                            OFFSET {KEEP_ROWS} LIMIT 1
                        )
                    """

                    cursor.execute(delete_query)
                    deleted_rows = cursor.rowcount
                    total_deleted += deleted_rows

                    if deleted_rows > 0:
                        print(f"{datetime.now()}: {table} - deleted {deleted_rows} rows (had {row_count}, keeping {KEEP_ROWS})")

            # Commit all deletions at once
            conn.commit()

            if total_deleted > 0:
                print(f"{datetime.now()}: Batch cleanup completed - {total_deleted} total rows deleted")

            return total_deleted

        except (Exception, psycopg2.Error) as error:
            print(f"{datetime.now()}: Error in batch cleanup: {error}")
            if conn:
                conn.rollback()
            return 0

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def run_periodic_cleanup(self, interval_hours=1):
        """Run batch cleanup periodically"""
        print(f"{datetime.now()}: Starting optimized periodic cleanup every {interval_hours} hour(s)")

        try:
            while self.running:
                self.cleanup_all_tables_batch()
                time.sleep(interval_hours * 60 * 60)

        except KeyboardInterrupt:
            print(f"\n{datetime.now()}: Cleanup stopped by user")
        finally:
            self.running = False

if __name__ == "__main__":
    # Use optimized approach for better performance
    cleaner = OptimizedDatabaseCleaner()

    # Run cleanup every hour, keeping most recent 100 rows per table
    cleaner.run_periodic_cleanup(interval_hours=1)
