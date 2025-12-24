import psycopg2
from psycopg2 import sql
import time
import threading

# Database connection details
source_db_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432',
}

target_db_params = {
    'dbname': 'short_data_hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432',
}

class DataTransferManager:
    def __init__(self, table_name):
        self.table_name = table_name
        self.target_table = f"{table_name}_short_data"
        self.source_connection = None
        self.target_connection = None
        self.source_cursor = None
        self.target_cursor = None
        self.last_transferred_timestamp = None
        self.columns = None
        self.running = True
        
    def connect_databases(self):
        """Establish connections to both databases"""
        try:
            self.source_connection = psycopg2.connect(**source_db_params)
            self.source_cursor = self.source_connection.cursor()
            
            self.target_connection = psycopg2.connect(**target_db_params)
            self.target_cursor = self.target_connection.cursor()
            
            # Get column names once
            self.source_cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (self.table_name,))
            self.columns = [row[0] for row in self.source_cursor.fetchall()]
            
            return True
            
        except Exception as error:
            print(f"[{self.table_name}] Connection error: {error}")
            return False
    
    def close_connections(self):
        """Close all database connections"""
        for conn in [self.source_cursor, self.target_cursor, self.source_connection, self.target_connection]:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def get_latest_row(self):
        """Get the latest row from table"""
        try:
            query = sql.SQL("SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1").format(
                sql.Identifier(self.table_name)
            )
            self.source_cursor.execute(query)
            return self.source_cursor.fetchone()
        except:
            return None
    
    def transfer_row(self, row_data):
        """Transfer a row to the target database"""
        try:
            insert_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({values})
            """).format(
                table=sql.Identifier(self.target_table),
                columns=sql.SQL(', ').join(map(sql.Identifier, self.columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(self.columns))
            )
            
            self.target_cursor.execute(insert_query, row_data)
            self.target_connection.commit()
            
            # Update last transferred timestamp
            timestamp_index = self.columns.index('timestamp') if 'timestamp' in self.columns else 1
            self.last_transferred_timestamp = row_data[timestamp_index]
            
            return True
            
        except Exception as error:
            print(f"[{self.table_name}] Transfer error: {error}")
            if self.target_connection:
                self.target_connection.rollback()
            return False
    
    def transfer_latest_row(self):
        """Transfer the latest row if it's new"""
        latest_row = self.get_latest_row()
        
        if not latest_row:
            return False
        
        # Check if this row has already been transferred
        timestamp_index = self.columns.index('timestamp') if 'timestamp' in self.columns else 1
        current_timestamp = latest_row[timestamp_index]
        
        if self.last_transferred_timestamp and current_timestamp == self.last_transferred_timestamp:
            return False
        
        return self.transfer_row(latest_row)
    
    def run_continuously(self, interval_seconds=0.018):
        """Run the transfer process continuously"""
        if not self.connect_databases():
            print(f"[{self.table_name}] Failed to connect. Exiting.")
            return
        
        try:
            while self.running:
                try:
                    self.transfer_latest_row()
                    time.sleep(interval_seconds)
                    
                except Exception as error:
                    print(f"[{self.table_name}] Error: {error}")
                    # Try to reconnect
                    self.close_connections()
                    time.sleep(1)
                    if not self.connect_databases():
                        break
                        
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            self.close_connections()
    
    def stop(self):
        """Stop the transfer process"""
        self.running = False

def run_all_transfers():
    """Run transfers for all tables using threading"""
    tables = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
    managers = []
    threads = []
    
    try:
        # Create managers and threads for each table
        for table in tables:
            manager = DataTransferManager(table)
            managers.append(manager)
            
            thread = threading.Thread(target=manager.run_continuously, args=(0.018,))
            thread.daemon = True
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        print("All transfers started. Press Ctrl+C to stop...")
        
        # Wait for all threads
        for thread in threads:
            thread.join()
            
    except KeyboardInterrupt:
        print("\nStopping all transfers...")
        for manager in managers:
            manager.stop()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=2)

if __name__ == "__main__":
    run_all_transfers()
