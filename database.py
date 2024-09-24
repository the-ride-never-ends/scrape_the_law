import asyncio
import re
import time
import traceback
from typing import Any, AsyncGenerator, LiteralString, Generator
import queue

import pandas as pd
import aiomysql

# These are primarily imported for errors and type-hinting purposes.
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection, CONNECTION_POOL_LOCK
from mysql.connector.errors import Error as MySqlError
from mysql.connector.errors import PoolError as MySqlPoolError

from aiomysql.pool import Pool as AioMySQLConnectionPool
from aiomysql.connection import Connection as AioMySqlConnection
from aiomysql.cursors import Cursor as AioMySQLCursor

from config import HOST, USER, PORT, PASSWORD, MYSQL_SCRIPT_FILE_PATH, DATABASE_NAME, INSERT_BATCH_SIZE
from logger import Logger
log_level = 10
logger = Logger(logger_name=__name__, log_level=log_level)

from utils.database.safe_format import safe_format
from utils.database.get_num_placeholders import get_num_placeholders
from utils.database.get_column_names import get_column_names
from utils.database.format_sql_file import format_sql_file
from utils.shared.make_id import make_id

# NOTE For reference only.
SOCIALTOOLKIT_TABLE_NAMES = [
    "locations", # Table of every incorporated village, town, city, and county in the US, along with geolocation data and domains. NOTE This is the seed dataset that will be fed to the web crawler.
    "doc_content", # Table of cleaned documents to be fed to an LLM
    "doc_metadata", # Table of metadata about the documents in doc_content
    "urls", # Table of crawled urls and their associated data. Domains are included, as they may have information to extract. NOTE This is where web crawler output goes.
    "ia_url_metadata", # Internet Archive (IA) copies of the URLs in 'url', as well as their IA metadata.
    "searches" # Searches we've conducted so far, as well as metadata on them.
]

# If the command starts with SELECT, SHOW, WITH, or EXPLAIN, it's a query.
QUERY_PATTERN = re.compile(r"^(SELECT|SHOW|WITH|EXPLAIN)\b", re.IGNORECASE) 

class MySqlDatabase:
    """
    Interact directly with a MySQL server via SQL commands and files.\n
    Supports synchronous and asynchronous queries (e.g. SELECT) and alteration commands (e.g. UPDATE, DELETE, INSERT, ALTER, etc).\n
    Can also run SQL commands directly and load them in from .sql files.

    ### Parameters
    - database: (str) Name of a MySQL database. Defaults to 'socialtoolkit'.
    - pool_name: (str) Name of the connection pool. Defaults to 'connection_pool'.
    - pool_size: (int) Number of connections to have in the connection pool. Defaults to 5.
    - port: (int) The server's port address. Defaults to yaml file configs.
    - user: (str) The username of the person using the server. Defaults to yaml file configs.
    - password: (str) The server's password. Defaults to yaml file configs.
    - host: (str) The server's host address. Typically 'localhost' or a remote host IP address. Defaults to yaml file configs.

    ### Instance Attributes
    - db_config: (dict) A dictionary of database configs. 
    - mysql_scripts_file_path: (str) The filepath to the MySQL scripts.
    - pool: (Awaitable[Callable]) A pool of connections to the MySQL server.
    - pool_name: (str) The name of open the connection pool.
    - pool_size: (int) The number of available connections in the connection pool.

    ### Methods
    #### Internal (Async)
    - _create_pool(): Creates a pool of connections to the MySQL server.
    - _async_get_connection_from_pool(): Get a connection from the pool.
    - _return_connection_to_pool(): Return a connection to the pool.
    - _async_execute_sql_command(): Execute a SQL command.

    #### External (Async)
    - async_connect_to_server(): Startup the connections pool to the server.
    - async_execute_sql_command(): Execute a SQL command via _async_execute_sql_command.
    - execute_sql_file(): Directly execute a SQL file via _async_execute_sql_command.
    - query_database(): Execute a simple SELECT query from the MySQL database.
    - alter_database(): Alter the MySQL database.
    - async_close_connection_to_server(): Close the connections pool.

    ### Example Usage
    >>> async with await MySqlDatabase(database="socialtoolkit") as db\n
    >>>     if query:
    >>>         return await db.async_execute_sql_command("SELECT * FROM links")\n
    >>>     else:
    >>>         await db.async_execute_sql_command("UPDATE links SET url = NULL WHERE ping_status = '404'")\n
    """

    def __init__(self, 
                 database: str="socialtoolkit", 
                 pool_name: str="connection_pool", 
                 pool_size: int=5,
                 pool_minsize: int=1,
                 pool_maxsize: int=64,
                 host: str=HOST,
                 user: str=USER,
                 port: int=PORT,
                 password: str=PASSWORD,
                 sql_scripts_path: str=MYSQL_SCRIPT_FILE_PATH # Currently not used.
                ):
        self.db_config: dict = {
            'host': host,
            'user': user,
            'port':  port,
            'password': password,
            'database': database if database else DATABASE_NAME
        }

        for config, value in self.db_config.items():
            if not value:
                logger.error(f"Invalid value for database config '{config}'.\n Value: {value}")
                raise ValueError("Invalid value for database config.")

        self.sql_scripts_path = sql_scripts_path
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.pool_minsize = pool_minsize
        self.pool_maxsize = pool_maxsize
        self.pool: AioMySQLConnectionPool|MySQLConnectionPool = None
        self.sync: bool = None

    def __enter__(self) -> 'MySqlDatabase':
        """
        Context manager entry method.
        Equivalent to db = MySqlDatabase().connect_to_server()
        """
        self.sync = True
        self._create_pool()
        return self


    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Context manager exit method.
        Equivalent to db.close_connection_to_server()
        """
        self.close_connection_to_server()


    async def __aenter__(self) -> 'MySqlDatabase':
        """
        Asynchronous context manager entry method.
        Equivalent to db = MySqlDatabase().async_connect_to_server()
        """
        self.sync = False
        await self._async_create_pool()
        return self


    async def __aexit__(self, exc_type, exc_value, traceback)  -> None: 
        """
        Asynchronous context manager exit method.
        Equivalent to db.async_close_connection_to_server()
        """
        await self.async_close_connection_to_server()


    @classmethod
    def connect_to_server(cls) -> 'MySqlDatabase':
        """
        Factory method to startup the connection to the server.
        This method cannot be used to create a `with` block.
        """
        instance = cls()
        instance._create_pool()
        instance.sync = True
        return instance


    @classmethod
    async def async_connect_to_server(cls) -> 'MySqlDatabase':
        """
        Async factory method to startup the connection to the server.
        This method cannot be used to create an `async with` block.
        """
        instance = cls()
        await instance._async_create_pool()
        instance.sync = False
        return instance


    def _create_pool(self) -> MySQLConnectionPool:
        """
        Create a pool of connections to the MySQL server.
        This reduces server overhead from constantly making connections and disconnections.

        ### Exceptions
        - ConnectionError: If there's any sort of error creating the pool.
        """
        try:
            logger.debug("Attempting to create MySQL server connection pool...")
            self.pool = MySQLConnectionPool(
                            pool_name=self.pool_name,
                            pool_size=self.pool_size,
                            **self.db_config
                        )
            logger.debug("MySQL server connection pool was successfully created.")
        except MySqlError as e:
            logger.error(f"Error creating MySQL server connection pool: {e}")
            traceback.print_exc()
            raise ConnectionError("Failed to create MySQL server connection pool.") from e


    async def _async_create_pool(self) -> AioMySQLConnectionPool:
        """
        Create an async pool of connections to the MySQL server.
        This reduces server overhead and helps prevent deadlocks caused by async code.

        ### Exceptions
        - ConnectionError: If there's any sort of error creating the pool.
        """
        try:
            logger.debug("Attempting to create async MySQL server connection pool...")
            self.pool = await aiomysql.create_pool(
                minsize=self.pool_maxsize,
                maxsize=self.pool_maxsize,
                loop=asyncio.get_event_loop(),
                host=self.db_config['host'],
                user=self.db_config['user'],
                port=self.db_config['port'],
                password=self.db_config['password'],
                db=self.db_config['database']
            )
            logger.debug("async MySQL server connection pool was successfully created.")
        except aiomysql.Error as e:
            logger.error(f"Error creating async MySQL server connection pool: {e}")
            traceback.print_exc()
            raise ConnectionError("Failed to create async MySQL server connection pool.") from e


    def _get_connection_from_pool(self) -> PooledMySQLConnection:
        """
        Get a connection from the connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error getting a connection from the pool.
        """
        try:
            return self.pool.get_connection()
        except aiomysql.Error as e:
            logger.exception(f"Failed to retrieve connection from the pool: {e}")
            traceback.print_exc()
            raise ConnectionError(f"No available connections in the pool: {e}") from e


    async def _async_get_connection_from_pool(self) -> aiomysql.connection.Connection:
        """
        Get a connection from the async connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error getting a connection from the pool.
        """
        try:
            return await self.pool.acquire()
        except MySqlPoolError as e:
            logger.exception(f"Failed to retrieve connection from the pool: {e}")
            traceback.print_exc()
            raise ConnectionError(f"No available connections in the pool: {e}") from e


    def _return_connection_to_pool(self, 
                                   connection: aiomysql.connection.Connection | PooledMySQLConnection
                                  ) -> None:
        """
        Return a connection to the connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error returning the connection.
        """
        if self.sync:
            logger.debug("Returning connection to pool...")
            self.pool.close()
            logger.debug("connection returned to pool.")
            return
        else:
            if not connection.closed:
                logger.debug("Returning async connection to pool...")
                self.pool.release(connection)
                logger.debug("Async connection returned to pool.")
                return
            else:
                logger.info("Async connection already closed. Releasing anyways as test...")
                try:
                    self.pool.release(connection)
                    return
                except Exception as e:
                    logger.exception(f"Failed to release async connection: {e}")
                    traceback.print_exc()
                    raise ConnectionError(f"Failed to release async connection: {e}") from e


    def close_connection_to_server(self) -> None:
        """
        Close the connection pool.\n
        NOTE Since MySQL-python doesn't have an explicit function to close the pool,\n
        we have to access the internals of the MySQLConnectionPool and close it manually.\n
        This might break in the future should Oracle change _cnx_queue the class.

        ### Exceptions
        - ConnectionError: If there's any sort of error closing the pool.
        """
        logger.debug(f"Attempting to close connection pool...")
        with CONNECTION_POOL_LOCK:
            pool_queue = self.pool._cnx_queue
            idx = 1 # Since we can't run enumerate in a while statement, we have to use a counter instead.
            while pool_queue.qsize():
                try:
                    logger.debug(f"Attempting to close connection {idx}...")
                    connection = pool_queue.get(block=False)
                    connection.disconnect()
                    logger.debug(f"Connection {idx} closed successfully.")
                    idx += 1
                except queue.Empty:
                    logger.info("Connection pool fully closed successfully.")
                    return
                except MySqlPoolError as e:
                    logger.exception(f"Failed to close the connection pool: {e}")
                    traceback.print_exc()
                    raise ConnectionError(f"Failed to close the connection pool: {e}") from e
            return


    async def async_close_connection_to_server(self) -> None:
        """
        Close the async connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error closing the pool.
        """
        try:
            logger.debug(f"Attempting to close async connection pool...")
            self.pool.close()
            logger.debug(f"Async connection pool closed. Waiting for full closure...")
            await self.pool.wait_closed()
            logger.info("Async connection pool fully closed successfully.")
            return
        except Exception as e:
            logger.exception(f"Failed to close the async connection pool: {e}")
            traceback.print_exc()
            raise ConnectionError(f"Failed to close the async connection pool: {e}") from e


    def _type_check_execute_sql_command(self, 
                                        connection, 
                                        command: LiteralString, 
                                        params: tuple|list[tuple]=None, 
                                        args: dict[Any]=None
                                        ):
        """
        Type check the _execute_sql_command and _async_execute_sql_command functions.
        TODO Fix type-checking for command as a LiteralString. Right now, "command" is being interpreted as just a string.
        """
        if not connection:
            logger.error("Database connection was not provided.")
            raise ValueError("Database connection was not provided.")

        if not command:
            logger.error("No SQL statement provided.")
            self._return_connection_to_pool(connection)
            raise ValueError("No SQL statement provided.")

        if params:
            if isinstance(params, tuple):
                is_tuple = True
            elif isinstance(params, list) and all(isinstance(item, tuple) for item in params):
                is_tuple = False
            # Force convert to tuples if it's a dictionary
            elif isinstance(params, dict):
                params = tuple(value for value in params.values())
                is_tuple = True
            elif isinstance(params, list) and all(isinstance(item, dict) for item in params):
                to_params = [
                    tuple(value for value in item.values()) for item in params
                ]
                params = to_params
                is_tuple = False
            else:
                self._return_connection_to_pool(connection)
                logger.error("Argument 'params' must be type tuple or list[tuple]")
                raise TypeError("Argument 'params' must be type tuple or list[tuple]")

        if args:
            for key, value in args.items():
                if isinstance(value, str):
                    args[key] = connection.escape_string(value)
                else:
                    args[key] = connection.escape(value)
            command = safe_format(command, **args)

        if params and args:
            return is_tuple, command
        else:
            if params:
                return is_tuple, None
            else:
                return None, command


    def _execute_sql_command(self, 
                            command: LiteralString, 
                            params: ( tuple[Any,...] | list[tuple[Any,...]] )=None,
                            connection: MySQLConnection=None,
                            is_query: bool=False,
                            args: dict=None,
                            unbuffered: bool=False,
                            return_dict: bool=False,
                            size: int=None
                            ) -> list[tuple[Any,...]] | list[dict[str,Any]] | aiomysql.Cursor | None:
    
        # Type check everything.
        is_tuple, command = self._type_check_execute_sql_command(connection, command, params=params, args=args)

        # Execute the SQL statement.
        try:
            with connection.cursor(buffered=unbuffered, dictionary=return_dict) as cursor:
                if is_query: # Query Database Route
                    logger.info(f"Querying database with '{command}'...")
                    if params: # Parameterized query
                        logger.debug(f"Params: '{command}'...")
                        if is_tuple and len(params) == 1:
                            cursor.execute(command, params)
                        else:
                            cursor.executemany(command, params) # Perform batching if params is a list of tuples or a tuple of tuples.
                    else: # Regular/Static query
                        cursor.execute(command)

                    logger.info(f"Query succesful. Returning results...")
                    if unbuffered: # For SSCursors, we return the cursor as the logic is handled in _execute_unbuffered_query.
                        return cursor
                    else: 
                        # Route fetch based on size parameter
                        # NOTE Since Cursor classes all have the same method names, these 3 commands are actually more like 12.
                        if size:
                            if size > 1:
                                results = cursor.fetchmany(size)
                            else: # NOTE if size == 1, size == 0, or size is negative, assume they wanted size 1.
                                results = cursor.fetchone()
                        else:
                            results = cursor.fetchall()
                        return results # Hopefully mysql isn't bugged like aiomysql

                else: # Alter Database Route
                    logger.info(f"Altering database with '{command}'...")
                    logger.debug("Creating server transaction...")
                    if params:
                        logger.debug(f"Params: '{command}'...")
                        if is_tuple and len(params) == 1:
                            cursor.execute(command, params)
                        else:
                            cursor.executemany(command, params) # Perform batching if params is a list of tuples or a tuple of tuples.
                    else:
                        cursor.execute(command)

                    logger.debug("Database alteration command executed. Committing server transaction...")
                    connection.commit() # Commit the transaction.
                    logger.info("Database alteration committed successfully.")
                    return

        except (MySqlError, Exception) as e:
            logger.error(f"Error executing SQL command '{command}': {e}")
            if not is_query:
                connection.rollback() # Rollback the database if there's an error altering it.
            cursor.close() # Shutdown the connection immediately if there's an error.
            self._return_connection_to_pool(connection)
            traceback.print_exc()
            raise e


    async def _async_execute_sql_command(self, 
                                   command: LiteralString, 
                                   params: ( tuple[Any,...] | list[tuple[Any,...]] )=None,
                                   connection: aiomysql.Connection=None,
                                   is_query: bool=False,
                                   args: dict=None,
                                   unbuffered: bool=False,
                                   return_dict: bool=False,
                                   size: int=None
                                  ) -> list[tuple[Any,...]] | list[dict[str,Any]] | aiomysql.Cursor | None:
        """
        Execute a SQL command, supporting both queries and database alterations.

        This method handles various types of SQL operations, including SELECT queries and
        Data Manipulation Language (DML) commands. It supports parameterized queries,
        safe string formatting, and different cursor types for flexible result handling.
    
        ### Parameters
        - command: The SQL command to execute.
        - params: Parameters for the SQL command. A tuple for single execution, or a list of tuples for batch execution.
        - connection: The database connection to use.
        - is_query: Whether the command is a query (True) or a database alteration (False).
        - args: Variables for safe string formatting of the SQL command.
        - unbuffered: Whether to use an unbuffered cursor.
        - return_dict: Whether to return results as dictionaries instead of tuples.
        - size: The number of rows to fetch for buffered queries. If default or None, fetches all rows.

        ### Returns
        - list[tuple[Any,...]]: For buffered queries, a list of tuples containing the query results.
        - dict[tuple[Any,...]]: For buffered queries with return_dict=True, a list of dictionaries containing the query results.
        - aiomysql.Cursor: For unbuffered queries, returns the cursor for further processing.
        - None: For database alteration commands (non-queries).

        ### Raises
        - ValueError: If no SQL statement or database connection is provided.
        - TypeError: If the params argument is of incorrect type.
        - aiomysql.Error: If there's an error executing the MySQL command.
        - Exception: For any other unexpected errors during execution.

        ### Notes
        - For queries, the method supports both parameterized and non-parameterized execution.
        - For database alterations, the method uses transactions and supports rollback in case of errors.
        """
        is_tuple, command = self._type_check_execute_sql_command(connection, command, params=params, args=args)

        # Define the cursor class based on the unbuffered and return_dict parameters.
        if unbuffered:
            cursor_class = aiomysql.SSDictCursor if return_dict else aiomysql.SSCursor
        else:
            cursor_class = aiomysql.DictCursor if return_dict else aiomysql.Cursor

        # Execute the SQL statement.
        try:
            async with connection.cursor(cursor_class) as cursor:
                if is_query: # Query Database Route
                    logger.info(f"Querying database with '{command}'...")
                    if params: # Parameterized query
                        logger.debug(f"Params: '{command}'...")
                        if is_tuple and len(params) == 1:
                            await cursor.execute(command, params)
                        else:
                            await cursor.executemany(command, params) # Perform batching if params is a list of tuples or a tuple of tuples.
                    else: # Regular/Static query
                        await cursor.execute(command)

                    logger.info(f"Query succesful. Returning results...")
                    if unbuffered: # for SSCursors, we return the cursor as the logic is handled in _execute_unbuffered_query.
                        return cursor
                    else: 
                        # Route fetch based on size parameter
                        # NOTE Since Cursor classes all have the same method names, these 3 commands are actually more like 12.
                        if size:
                            if size > 1:
                                results = await cursor.fetchmany(size)
                            else: # NOTE if size == 1, size == 0, or size is negative, assume they wanted size 1.
                                results = await cursor.fetchone()
                        else:
                            results = await cursor.fetchall()

                        # Aiomysql is bugged, so that its fetch commands under class Cursor
                        # return a tuple of tuples instead of a list of tuples.
                        # So we gotta convert it here.
                        if isinstance(results, list):
                            return results
                        else:
                            if isinstance(results, dict):
                                return results
                            else:
                                return list(results)

                else: # Alter Database Route
                    logger.info(f"Altering database with '{command}'...")
                    logger.debug("Creating server transaction...")
                    await connection.begin() # Create a transaction. This prevents commands from being automatically executed.
                    if params:
                        logger.debug(f"Params: '{command}'...")
                        if is_tuple and len(params) == 1:
                            await cursor.execute(command, params)
                        else:
                            await cursor.executemany(command, params) # Perform batching if params is a list of tuples or a tuple of tuples.
                    else:
                        await cursor.execute(command)

                    logger.debug("Database alteration command executed. Committing server transaction...")
                    await connection.commit() # Commit the transaction.
                    logger.info("Database alteration committed successfully.")
                    return

        except (aiomysql.Error, Exception) as e:
            logger.error(f"Error executing SQL command '{command}': {e}")
            if not is_query:
                await connection.rollback() # Rollback the database if there's an error altering it.

            # Shutdown the connection immediately if there's an error.
            await cursor.close()
            self._return_connection_to_pool(connection)
            traceback.print_exc()
            raise e


    def _execute_unbuffered_query(self,
                                    command: LiteralString,
                                    params: (tuple[Any,...] | list[tuple[Any,...]]) = None,
                                    connection: MySQLConnection=None,
                                    is_query:bool=True,
                                    args: dict=None,
                                    unbuffered: bool=True,
                                    return_dict: dict=False,
                                    size: int=None
                                    ) -> Generator | None:
        """
        Execute an unbuffered SQL query and yield results as a generator.

        Allows processing large result sets without loading all into memory at once.
        """
        logger.debug("Executing unbuffered query...")
        try:
            cursor = self._async_execute_sql_command(
                command, params=params, connection=connection, is_query=is_query,
                args=args, unbuffered=unbuffered, return_dict=return_dict, size=size
            )
            for row in cursor:
                print(row)
                yield row
        finally:
            cursor.close()
            self._return_connection_to_pool(connection)


    async def _async_execute_unbuffered_query(self,
                                    command: LiteralString,
                                    params: (tuple[Any,...] | list[tuple[Any,...]]) = None,
                                    connection: aiomysql.Connection=None,
                                    is_query:bool=True,
                                    args: dict=None,
                                    unbuffered: bool=True,
                                    return_dict: dict=False,
                                    size: int=None
                                    ) -> AsyncGenerator | None:
        """
        Asynchronously cxecute an unbuffered SQL query and yield results asynchronously.

        Allows processing large result sets without loading all into memory at once.

        ### Parameters
        - command (LiteralString): The SQL query to execute.
        - params (tuple[Any,...] | list[tuple[Any,...]], optional): Parameters to be used with the SQL query.
        - connection (aiomysql.Connection, optional): The database connection to use.
        - is_query (bool, default=True): Indicates if the command is a query (should always be True for this method).
        - args (dict, optional): Variables for safe string formatting of the SQL command.
        - unbuffered (bool, default=True): Indicates if the query should be unbuffered (should always be True for this method).
        - return_dict (bool, default=False): If True, returns each row as a dictionary instead of a tuple.
        - size (int, optional): Not used in this method, but kept for consistency with _async_execute_sql_command.

        ### Yields
        - row: Each row of the query result. The type depends on the return_dict parameter:
            - If return_dict is True: dict[str, Any]
            - If return_dict is False: tuple[Any, ...]
        """
        logger.debug("Executing async unbuffered query...")
        try:
            cursor = await self._async_execute_sql_command(
                command, params=params, connection=connection, is_query=is_query, 
                args=args, unbuffered=unbuffered, return_dict=return_dict, size=size
            )
            async for row in cursor:
                print(row)
                yield row
        finally:
            await cursor.close()
            self._return_connection_to_pool(connection)


    # def _route_commands(self, command: str, is_query: bool=None, unbuffered: bool=False, **kwargs):
    #     connection: MySQLConnection = self.pool.get_connection()
    #     is_query = is_query or bool(QUERY_PATTERN.match(command.strip()))

    #     # Execute the SQL command.
    #     if is_query: # Query route
    #         if unbuffered: # Unbuffered query
    #             logger.debug(f"Chose unbuffered query route.")
    #             return self._async_execute_unbuffered_query(command, connection=connection, unbuffered=unbuffered, is_query=is_query, **kwargs)
    #         else: # Buffered query
    #             logger.debug(f"Chose buffered query route.")
    #             results = self._execute_sql_command(command,  connection=connection, is_query=is_query, **kwargs)
    #             self._return_connection_to_pool(connection)
    #             return results

    #     else: # Alteration route
    #         self._execute_sql_command(command, connection=connection, **kwargs)
    #         self._return_connection_to_pool(connection)
    #         return


    # def execute_sql_command(self,
    #                         command: LiteralString,
    #                         params: ( tuple[Any,...] | dict[str,Any] | list[tuple[Any,...]] | list[dict[str,Any]] ) = None,
    #                         args: dict=None,
    #                         unbuffered:bool=False,
    #                         return_dict:bool=False,
    #                         size: int=None,
    #                         ) -> list[tuple[Any,...]] | list[dict[str,Any]] | Generator | None:
    #     """
    #     Directly execute a SQL command to query or alter the database.
    #     This method automatically detects whether the command is a query or an alteration command based on its first word.
    #     It handles both buffered and unbuffered queries.

    #     ### Parameters
    #     - command: SQL command to execute.
    #     - params: Parameters/values to feed into the SQL command. Optional
    #     - args: Variable names and values that are inserted into the command string using the safe_format function. Optional
    #     - unbuffered: If True, executes an unbuffered query, allowing for processing large result sets without loading the entire result into memory at once.
    #     - return_dict: If True, returns each row as a dictionary instead of a tuple.
    #     - size: Number of rows to fetch at a time. If None, fetches all rows.

    #     ### Returns
    #     - For buffered queries, returns a list of tuples containing the query results.
    #     - For buffered queries with return_dict=True, returns a list of dictionaries containing the query results.
    #     - For unbuffered queries, returns an async generator that yields results one at a time.
    #     - For database alteration commands (non-queries), returns None.
    #     """
        

    #     if command.endswith((".sql", ".txt")):
    #         command = format_sql_file(command)
    #         if isinstance(command, str):
    #             is_query = bool(QUERY_PATTERN.match(command.strip()))
    #             if is_query:
    #                 return self._route_commands(command, is_query, unbuffered=unbuffered, params=params, args=args,  return_dict=return_dict)
    #             else:
    #                 return self._route_commands(command, is_query, unbuffered=unbuffered, params=params, args=args,  return_dict=return_dict)
    #         else:
    #             command_set = {bool(pattern.match(com.strip())) for com in command}
    #             if True and False in command_set:
    #                 raise ValueError("File has both query and alteration commands.")
    #             else:
    #                 for com in command:
    #                     is_query = bool(QUERY_PATTERN.match(command.strip()))
    #     else:
    #         is_query = bool(QUERY_PATTERN.match(command.strip()))
    #         if is_query:
    #             return self._route_commands(command, is_query)
    #         else:


    async def async_insert_by_batch(self,
                                    results: list[dict] | list[tuple],
                                    batch_size: int=INSERT_BATCH_SIZE,
                                    args: dict=None,
                                    columns:list[str]=None,
                                    statement:str=None,
                                    table: str=None) -> None:
        """
        Asynchronously insert data into the database in batches.

        ### Args
        - results: Data to be inserted.
        - batch_size: Number of records per batch.
        - args: Additional arguments for the SQL statement.
        - columns: Column names if results is a list of tuples.
        - statement: Custom INSERT statement.
        - table: Name of the table to insert into.

        Raises
        - AssertionError: If input types are incorrect.
        """
        if not columns:
            logger.debug(f"type results: {type(results)}")
            logger.debug(f"results: {results}")
            time.sleep(10)
            #assert isinstance(results[0], dict), f"type results: {type(results)}\nresults: {results}"
        else:
            assert isinstance(columns, list)
            assert isinstance(columns[0], str)

        args = args or {
            "table": table or None,
            "placeholders": get_num_placeholders(results[0]),
            "columns": columns or get_column_names(results[0])
        }
        insert = statement or "INSERT INTO {table} ({columns}) VALUES ({placeholders});"

        total_inserted = 0
        for i in range(0, len(results), batch_size):
            params = results[i:i+batch_size]
            try:
                await self.async_execute_sql_command(insert, params=params, args=args)
                total_inserted += len(params)
                logger.info(f"Inserted {len(params)} records into the database. Total: {total_inserted}")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                # Save batched results in CSV in case something goes wrong
                csv_filename = f"failed_insert_batch_{i}_{make_id()}.csv"
                try: # Try one more time...
                    await self.async_execute_sql_command(insert, params=params, args=args)
                except:
                    pass
                try:
                    results = pd.DataFrame(params).to_csv(csv_filename, index=False)
                    logger.info(f"Saved failed batch to {csv_filename}")
                except Exception as csv_error:
                    logger.error(f"Failed to save batch to CSV: {csv_error}")
                raise  # Re-raise the original exception
        logger.info(f"Insertion complete. Total records inserted: {total_inserted}")

    async def async_execute_sql_command(self,
                                  command: LiteralString,
                                  params: ( tuple[Any,...] | dict[str,Any] | list[tuple[Any,...]] | list[dict[str,Any]] ) = None,
                                  args: dict=None,
                                  unbuffered:bool=False,
                                  return_dict:bool=False,
                                  size: int=None,
                                 ) -> list[tuple[Any,...]] | list[dict[str,Any]] | AsyncGenerator | None:
        """
        Directly execute an asynchronous SQL command to query or alter the database.
        This method automatically detects whether the command is a query or an alteration command based on its first word.
        It handles both buffered and unbuffered queries.

        ### Parameters
        - command: SQL command to execute.
        - params: Parameters/values to feed into the SQL command. Optional
        - args: Variable names and values that are inserted into the command string using the safe_format function. Optional
        - unbuffered: If True, executes an unbuffered query, allowing for processing large result sets without loading the entire result into memory at once.
        - return_dict: If True, returns each row as a dictionary instead of a tuple.
        - size: Number of rows to fetch at a time. If None, fetches all rows.

        ### Returns
        - For buffered queries, returns a list of tuples containing the query results.
        - For buffered queries with return_dict=True, returns a list of dictionaries containing the query results.
        - For unbuffered queries, returns an async generator that yields results one at a time.
        - For database alteration commands (non-queries), returns None.
        """

        connection: AioMySqlConnection = await self._async_get_connection_from_pool()

        pattern = re.compile(r"^(SELECT|SHOW|WITH|EXPLAIN)\b", re.IGNORECASE) # If the command starts with SELECT, SHOW, WITH, or EXPLAIN, it's a query.
        is_query = bool(pattern.match(command.strip()))

        # Execute the SQL command.
        if is_query: # Query route
            if unbuffered: # Unbuffered query
                logger.debug(f"Chose unbuffered query route.")
                return await self._async_execute_unbuffered_query(command, params=params, connection=connection, is_query=is_query, 
                                                                args=args, return_dict=return_dict, unbuffered=unbuffered, size=size)
            else: # Buffered query
                logger.debug(f"Chose buffered query route.")
                results = await self._async_execute_sql_command(command, params=params, connection=connection, is_query=is_query, 
                                                                args=args, return_dict=return_dict, size=size)
                self._return_connection_to_pool(connection)
                return results

        else: # Alteration route
            await self._async_execute_sql_command(command, params=params, connection=connection, args=args)
            self._return_connection_to_pool(connection)
            return


    async def async_query_to_dataframe(self,
                                    query: str,
                                    params: tuple = None,
                                    args: dict = None,
                                    unbuffered=False
                                    ) -> pd.DataFrame:
        """
        Execute an async MySQL query and return results as a Pandas DataFrame.

        ### Parameters
        - query: SQL query to execute.
        - params: Query parameters.
        - args: Variables for safe string formatting.
        - unbuffered: If True, uses unbuffered query.
        ### Returns
        - Query results as a Pandas DataFrame.
        """
        results = await self.async_execute_sql_command(query, params=params, unbuffered=unbuffered, args=args, return_dict=True)
        return pd.DataFrame.from_dict(results)


    def query_to_dataframe(self,
                            query: str,
                            params: tuple = None,
                            args: dict = None,
                            unbuffered: bool = False
                            ) -> pd.DataFrame:
        """
        Execute a MySQL query and return results as a Pandas DataFrame.

        ### Parameters
        - query: SQL query to execute.
        - params: Query parameters.
        - args: Variables for safe string formatting.
        - unbuffered: If True, uses unbuffered query.
        ### Returns
        - Query results as a Pandas DataFrame.
        """
        results = self.execute_sql_command(query, params=params, unbuffered=unbuffered, args=args, return_dict=True)
        return pd.DataFrame.from_dict(results)

