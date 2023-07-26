import pandas as pd
import mysql.connector
import csv


class DatabaseConnection:
    def __init__(self, host: str, database: str, username: str, password: str):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
            )
            print("Connected to the database.")
        except mysql.connector.Error as e:
            print("Error connecting to the database:", str(e))

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            print("Disconnected from the database.")

    def execute_query(self, query: str):
        try:
            self._assert_connection()
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully.")
        except mysql.connector.Error as e:
            print("Error executing query:", str(e))

    def fetch_data(self, query: str):
        try:
            self._assert_connection()
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            result = pd.DataFrame(data, columns=columns)
            return result
        except mysql.connector.Error as e:
            print("Error fetching data:", str(e))

    def save_to_csv(self, data: pd.DataFrame, file_name: str):
        try:
            data.to_csv(file_name, index=False)
            print(f"Data saved to CSV: {file_name}")
        except IOError:
            print("Error saving data to CSV.")

    def save_to_excel(self, data: pd.DataFrame, file_name: str, sheet_name: str):
        try:
            writer = pd.ExcelWriter(file_name, engine="xlsxwriter")
            data.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.close()
            print(f"Data saved to Excel: {file_name} - Sheet: {sheet_name}")
        except IOError:
            print("Error saving data to Excel.")

    def _assert_connection(self):
        assert self.connection is not None, "Not connected to the database."


db = DatabaseConnection("localhost", "myflixdb", "root", "")
db.connect()

db.execute_query(
    "INSERT INTO Categories (category_name, remarks) VALUES ('Drama', 'a play, opera, mime, ballet')"
)

result = db.fetch_data("SELECT * FROM Categories")
print(result)

db.save_to_csv(result, "Categories.csv")
db.save_to_excel(result, "Categories.xlsx", "Sheet1")

db.disconnect()
