import os
import pyodbc
from datetime import datetime
from source.logging_modules import CustomLogger

class DatabaseConnection:
    """
    Manages a single database connection lifecycle: connect on init,
    and provide a close() method to terminate the connection.
    """

    def __init__(self, db_server_ip: str, db_server_port: str, db_name: str, db_user: str, db_password: str):
        """
        Initialize the DatabaseConnection with the required parameters.
        
        :param db_server_ip: IP or hostname of the SQL Server
        :param db_server_port: Port on which SQL Server is listening (commonly 1433)
        :param db_name: Database name to connect to
        :param db_user: Login username
        :param db_password: Login password
        """
        self.logger = CustomLogger(__name__).get_logger()
        self.db_server_ip = db_server_ip
        self.db_server_port = db_server_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.connection = self.connect()

    def connect(self) -> pyodbc.Connection:
        """
        Establish a connection to the database, setting autocommit=False
        and the desired isolation level (READ COMMITTED by default).
        
        :return: A pyodbc.Connection object if successful.
        :raises: pyodbc.Error if the connection fails.
        """
        db_driver = os.environ.get('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        db_timeout = os.environ.get('DB_TIMEOUT', '30')

        conn_str = (
            f"Driver={{{db_driver}}};"
            f"Server={self.db_server_ip},{self.db_server_port};"
            f"Database={self.db_name};"
            f"UID={self.db_user};"
            f"PWD={self.db_password};"
            f"Connection Timeout={db_timeout};"
        )

        try:
            conn = pyodbc.connect(conn_str)
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            self.logger.info(f"[bright_black][DbConnection]🔗[/bright_black] Connected to database: {self.db_name}")
            return conn
        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbConnection]🔗[/bright_black]: {e}")
            raise

    def close(self):
        """
        Close the database connection if it is open.
        """
        if self.connection:
            self.connection.close()
            self.logger.info(f"[bright_black][DbConnection]🔗[/bright_black] Connection to database '{self.db_name}' closed.")


class DatabaseManager:
    """
    Provides database operations, each accepting a pyodbc.Connection
    so that you can use any connection without storing it in the manager.
    """

    def __init__(self):
        self.logger = CustomLogger(__name__).get_logger()

    def check_table_exists(self, connection: pyodbc.Connection, table_name: str) -> bool:
        """
        Check if the specified table exists in 'dbo' schema.
        
        :param connection: An active pyodbc.Connection object
        :param table_name: The table name to check
        :return: True if the table exists, otherwise False
        """
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
            """, (table_name,))
            return cursor.fetchone() is not None
        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black] Error checking if table '{table_name}' exists: {e}")
            raise

    def drop_table(self, connection: pyodbc.Connection, table_name: str) -> None:
        """
        Drop the specified table (if it exists).
        
        :param connection: An active pyodbc.Connection object
        :param table_name: Name of the table to drop
        :raises: pyodbc.Error on DB errors
        """
        try:
            cursor = connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS [dbo].[{table_name}];")
            connection.commit()
            self.logger.info(f"[bright_black][DbManager]🗃️[/bright_black] Table '{table_name}' dropped successfully.")
        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black] Error dropping table '{table_name}': {e}")
            connection.rollback()
            raise

    def create_table(self, connection: pyodbc.Connection, table_name: str) -> bool:
        """
        Create a new table if it does not exist. 
        Returns True if a new table is created, False if it already exists.
        
        :param connection: An active pyodbc.Connection object
        :param table_name: Name of the table to create
        :return: True if the table was created, False if it already existed
        :raises: pyodbc.Error on DB errors
        """
        if self.check_table_exists(connection, table_name):
            self.logger.info(f"[bright_black][DbManager]🗃️[/bright_black] Table '{table_name}' already exists. Skipping creation.")
            return False

        try:
            cursor = connection.cursor()
            cursor.execute(f"""
                CREATE TABLE [dbo].[{table_name}](
                    [file_path]         NVARCHAR(1024) NOT NULL,
                    [file_name]         NVARCHAR(256)  NOT NULL,
                    [file_directory]    NVARCHAR(128)  NOT NULL,
                    [file_type]         NVARCHAR(64)   NOT NULL,
                    [file_extension]    NVARCHAR(8)    NOT NULL,
                    [file_size]         BIGINT         NOT NULL,
                    [md5]               CHAR(64)       NULL,
                    [sha256]            CHAR(256)       NULL,
                    [sha512]            CHAR(512)      NULL,
                    [blake3]            CHAR(512)       NOT NULL,
                    [dhash]             CHAR(256)       NULL,
                    [phash]             CHAR(256)       NULL,
                    [whash]             CHAR(256)       NULL,
                    [chash]             CHAR(256)       NULL,
                    [ahash]             CHAR(256)       NULL,
                    [video_fingerprint] CHAR(512)       NULL,
                    [video_width]       INT            NULL,
                    [video_height]      INT            NULL,
                    [video_resolution]  VARCHAR(16)    NULL,
                    [video_fps]         INT            NULL,
                    [video_length]      FLOAT          NULL,
                    [has_human]         BIT            NOT NULL DEFAULT 0,
                    [has_human_score]   DECIMAL(5, 2)  NULL,
                    [has_human_count]   INT            NULL,
                    [date]              DATETIME       NOT NULL DEFAULT GETDATE()
                );
            """)
            cursor.execute(f"""
                ALTER TABLE [dbo].[{table_name}] 
                ADD CONSTRAINT [UQ_{table_name}_blake3] UNIQUE NONCLUSTERED ([blake3] ASC);
            """)
            cursor.execute(f"""
                ALTER TABLE [dbo].[{table_name}] 
                ADD CONSTRAINT [UQ_{table_name}_file_path] UNIQUE NONCLUSTERED ([file_path] ASC);
            """)
            for hash_type in ['dhash', 'phash', 'whash', 'chash', 'ahash']:
                cursor.execute(f"""
                    CREATE NONCLUSTERED INDEX [IX_{table_name}_{hash_type}]
                    ON [dbo].[{table_name}] ([{hash_type}] ASC);
                """)
            connection.commit()
            self.logger.info(f"[bright_black][DbManager]🗃️[/bright_black][bold green] Table '{table_name}' created successfully.[/bold green]")
            return True
        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black] Error creating table '{table_name}': {e}")
            connection.rollback()
            raise

    def reset_table(self, connection: pyodbc.Connection, table_name: str) -> None:
        """
        Drop and recreate the table. 
        :param connection: An active pyodbc.Connection object
        :param table_name: Name of the table to reset
        """
        self.drop_table(connection, table_name)
        self.create_table(connection, table_name)
        self.logger.info(f"[bright_black][DbManager]🗃️[/bright_black] Table '{table_name}' reset successfully.")

    def insert(
        self,
        connection: pyodbc.Connection,
        table_name: str,
        file_path: str = "",
        file_directory: str = "",
        file_name: str = "",
        file_type: str = "",
        file_extension: str = "",
        file_size: int = 0,
        blake3: str = "",
        has_human: bool = False,
        md5: str = None,
        sha256: str = None,
        sha512: str = None,
        dhash: str = None,
        phash: str = None,
        whash: str = None,
        chash: str = None,
        ahash: str = None,
        video_fingerprint: str = None,
        video_width: int = None,
        video_height: int = None,
        video_resolution: str = None,
        video_fps: int = None,
        video_length: float = None,
        has_human_score: float = 0.0,
        has_human_count: int = 0
    ) -> bool:
        """
        Insert data into the specified table. Returns True if insertion 
        is successful, False if there's a constraint violation or missing fields.
        """
        # Validate required fields
        if not file_path or not file_name or not file_type or not blake3:
            self.logger.error("[bright_black][DbManager]🗃️[/bright_black][bold red]Missing required fields: file_path, file_name, file_type, and blake3.[/bold red]")
            return False

        # 1) Optional check for duplicates (if you want to manually skip)
        if self.check_table_exists(connection, table_name):
            existing_record = connection.cursor().execute(
                f"SELECT COUNT(*) FROM [dbo].[{table_name}] WHERE [blake3] = ? OR [file_path] = ?",
                (blake3, file_path)
            ).fetchone()[0]
            
            if existing_record > 0:
                self.logger.warning(f"[bright_black][DbManager]🗃️[/bright_black][yellow] Duplicate entry for blake3 or file_path: {file_path}.[/yellow] [bold blue]Insertion skipped.[/bold blue]")
                return False

        try:
            cursor = connection.cursor()

            # 2) Only include the columns we always need
            columns = [
                "file_path",
                "file_directory",
                "file_name",
                "file_type",
                "file_extension",
                "file_size",
                "blake3",
                "has_human",
            ]
            values = [
                file_path,
                file_directory,
                file_name,
                file_type,
                file_extension,
                file_size,
                blake3,
                has_human,
            ]

            # 3) Append only the optional fields that are not None
            optional_fields = [
                ("md5", md5),
                ("sha256", sha256),
                ("sha512", sha512),
                ("dhash", dhash),
                ("phash", phash),
                ("whash", whash),
                ("chash", chash),
                ("ahash", ahash),
                ("video_fingerprint", video_fingerprint),
                ("video_width", video_width),
                ("video_height", video_height),
                ("video_resolution", video_resolution),
                ("video_fps", video_fps),
                ("video_length", video_length),
                ("has_human_score", has_human_score),
                ("has_human_count", has_human_count)
            ]

            for field, value in optional_fields:
                if value is not None:
                    columns.append(field)
                    values.append(value)

            # 4) Build the final SQL
            placeholders = ", ".join(["?"] * len(columns))
            col_names = ", ".join([f"[{c}]" for c in columns])
            sql = f"INSERT INTO [dbo].[{table_name}] ({col_names}) VALUES ({placeholders})"

            cursor.execute(sql, values)
            connection.commit()

            self.logger.debug(f"[bright_black][DbManager]🗃️[/bright_black][bold #FFA500] Record inserted successfully: {file_path}[/bold #FFA500]")
            return True

        except pyodbc.IntegrityError as e:
            error_msg = str(e)
            if f"UQ_{table_name}_blake3" in error_msg:
                self.logger.warning(
                    f"[bright_black][DbManager]🗃️[/bright_black][bold yellow]Duplicate BLAKE3 hash, skipping: {file_path} (hash: {blake3})[/bold yellow]"
                )
                return False
            elif f"UQ_{table_name}_file_path" in error_msg:
                self.logger.warning(
                    f"[bright_black][DbManager]🗃️[/bright_black][bold yellow]Duplicate file path, skipping: {file_path}[/bold yellow]"
                )
                return False
            elif "violation of UNIQUE KEY constraint" in error_msg:
                self.logger.warning(
                    f"[bright_black][DbManager]🗃️[/bright_black][bold yellow]Unique constraint violation, skipping: {file_path}[/bold yellow]"
                )
                return False
            elif "violation of PRIMARY KEY constraint" in error_msg:
                self.logger.warning(
                    f"[bright_black][DbManager]🗃️[/bright_black][bold yellow]Primary key violation, skipping: {file_path}[/bold yellow]"
                )
                return False
            else:
                self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black][bold red]Integrity error inserting record: {e}[/bold red]")
                connection.rollback()
                return False

        except pyodbc.DataError as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black][bold red]Data error inserting record: {e}[/bold red]")
            connection.rollback()
            return False

        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black][bold red]Database error inserting record: {e}[/bold red]")
            connection.rollback()
            return False

        except Exception as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black][bold red]Unexpected error inserting record: {e}[/bold red]")
            connection.rollback()
            return False

    def fetch(self, connection: pyodbc.Connection, query: str):
        """
        Execute a SELECT query and return all rows.
        
        :param connection: An active pyodbc.Connection object
        :param query:      SQL query to execute
        :return:           List of rows (each row is typically a pyodbc.Row)
        :raises:           pyodbc.Error on DB errors
        """
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except pyodbc.Error as e:
            self.logger.error(f"[bright_black][DbManager]🗃️[/bright_black]Error fetching data with query '{query}': {e}")
            raise
        
    def exists_by_blake3(self, connection, table_name: str, blake3: str) -> bool:
        query = f"SELECT COUNT(*) FROM [dbo].[{table_name}] WHERE blake3 = ?"
        with connection.cursor() as cursor:
            cursor.execute(query, (blake3,))
            (count,) = cursor.fetchone()
        return count > 0

if __name__ == "__main__":
    pass