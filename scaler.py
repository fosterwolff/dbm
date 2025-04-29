import psycopg2
from psycopg2 import sql

class DatabaseManager:
    def __init__(self, user, password, host='localhost', port='5432'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def create_database(self, dbname):
        try:
            # Connect to the default database (postgres)
            connection = psycopg2.connect(
                dbname='postgres',  # Connect to the default postgres database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            
            # Set autocommit to True to allow CREATE DATABASE outside of a transaction
            connection.autocommit = True

            # Create a cursor object
            cursor = connection.cursor()

            # SQL command to create a new database
            create_db_query = f"CREATE DATABASE {dbname}"

            # Execute the query
            cursor.execute(create_db_query)

            print(f"Database '{dbname}' created successfully!")

        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def list_databases(self):
        try:
            # Connect to the default database (postgres)
            connection = psycopg2.connect(
                dbname='postgres',  # Connect to the default postgres database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )

            # Create a cursor object
            cursor = connection.cursor()

            # SQL command to list all databases
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")

            # Fetch all results
            databases = cursor.fetchall()

            # Return the list of database names
            return [db[0] for db in databases]

        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def switch_database(self, dbname):
        try:
            # Connect to the selected database
            self.connection = psycopg2.connect(
                dbname=dbname,  # Connect to the specified database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print(f"Successfully switched to database '{dbname}'.")

        except Exception as e:
            print(f"Error: {e}")

    def create_table(self, table_name, columns=None):
        """
        Creates a new table in the current database.
        
        :param table_name: Name of the table to create
        :param columns: List of tuples where each tuple is (column_name, column_type)
                        If None, an empty table is created.
        """
        try:
            if not self.connection or not self.cursor:
                print("Not connected to any database.")
                return

            if columns:
                # Create the table creation query dynamically with columns
                columns_query = ', '.join([f"{col[0]} {col[1]}" for col in columns])
                create_table_query = sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(columns_query)
                )
            else:
                # Create an empty table if no columns are specified
                create_table_query = sql.SQL("CREATE TABLE {} ()").format(
                    sql.Identifier(table_name)
                )

            # Execute the query
            self.cursor.execute(create_table_query)
            self.connection.commit()

            print(f"Table '{table_name}' created successfully!")

        except Exception as e:
            print(f"Error: {e}")

    def add_column(self, dbname, table_name, column_name, column_type):
        """
        Adds a new column to the specified table in the specified database.
        
        :param dbname: Name of the database
        :param table_name: Name of the table to add the column to
        :param column_name: Name of the column to add
        :param column_type: Data type of the column (e.g., 'VARCHAR(100)', 'INTEGER')
        """
        try:
            # Switch to the specified database
            self.switch_database(dbname)
            
            # SQL query to add the new column to the table
            add_column_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.SQL(column_type)
            )

            # Execute the query
            self.cursor.execute(add_column_query)
            self.connection.commit()

            print(f"Column '{column_name}' of type '{column_type}' added to table '{table_name}'.")

        except Exception as e:
            print(f"Error: {e}")

    def insert_row(self, dbname, table_name, column_values):
        """
        Inserts a new row into the specified table in the specified database.
        
        :param dbname: Name of the database
        :param table_name: Name of the table to insert the row into
        :param column_values: Dictionary with column names as keys and the corresponding values to insert
        Example: {'name': 'John Doe', 'age': 30}
        """
        try:
            # Switch to the specified database
            self.switch_database(dbname)
            
            # Split column names and values from the dictionary
            columns = column_values.keys()
            values = column_values.values()

            # Create the insert query dynamically
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),  # Join column names
                sql.SQL(', ').join(map(sql.Literal, values))  # Join values
            )

            # Execute the insert query
            self.cursor.execute(insert_query)
            self.connection.commit()

            print(f"Row inserted into table '{table_name}'.")

        except Exception as e:
            print(f"Error: {e}")

    def delete_row(self, dbname, table_name, row_id):
        """
        Deletes a row from the specified table in the specified database using the row's id.
        
        :param dbname: Name of the database
        :param table_name: Name of the table to delete the row from
        :param row_id: The id of the row to delete
        """
        try:
            # Switch to the specified database
            self.switch_database(dbname)
            
            # SQL query to delete the row based on the id
            delete_query = sql.SQL("DELETE FROM {} WHERE id = %s").format(
                sql.Identifier(table_name)
            )

            # Execute the query with the row_id as parameter
            self.cursor.execute(delete_query, (row_id,))
            self.connection.commit()

            print(f"Row with id {row_id} deleted from table '{table_name}'.")

        except Exception as e:
            print(f"Error: {e}")

    def update_row(self, dbname, table_name, row_id, update_data):
        """
        Updates the contents of a row in the specified table based on the row's id.
        
        :param dbname: Name of the database
        :param table_name: Name of the table to update the row in
        :param row_id: The id of the row to update
        :param update_data: Dictionary where keys are the field names and values are the new values to set
        Example: {'name': 'Jane Doe', 'age': 31}
        """
        try:
            # Switch to the specified database
            self.switch_database(dbname)
            
            # Dynamically create the SET clause of the update query
            set_clause = ', '.join([f"{key} = %s" for key in update_data.keys()])

            # Create the update query
            update_query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(
                sql.Identifier(table_name),
                sql.SQL(set_clause)
            )

            # Values to update, including the row_id
            values = list(update_data.values()) + [row_id]

            # Execute the query
            self.cursor.execute(update_query, values)
            self.connection.commit()

            print(f"Row with id {row_id} updated in table '{table_name}'.")

        except Exception as e:
            print(f"Error: {e}")

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Example usage
db_manager = DatabaseManager(user='postgres', password='postgres')

# List all databases
databases = db_manager.list_databases()

# Print all databases
print("Databases:")
for db in databases:
    print(db)

# Switch to a database (for example, 'new_db')
db_manager.switch_database('new_db')

# Create a new table in the 'new_db' database
db_manager.create_table('person', columns=[('id', 'SERIAL PRIMARY KEY'), ('name', 'VARCHAR(100)'), ('age', 'INTEGER')])

# Insert a new row into the 'person' table
column_values = {'name': 'John Doe', 'age': 30}
db_manager.insert_row('new_db', 'person', column_values)

# Update the row with id 1 in the 'person' table
update_data = {'name': 'Jane Doe', 'age': 31}
db_manager.update_row('new_db', 'person', 2, update_data)

# After use, close the connection
db_manager.close_connection()
