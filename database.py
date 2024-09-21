import asyncio
import re
import string
import sys
import traceback
from typing import AsyncGenerator, Any, List, Dict, LiteralString, Tuple

try:
    import pandas as pd
    import aiomysql
    import mysql.connector
except ModuleNotFoundError as e:
    module_name = e.name
    raise ModuleNotFoundError(f"Could not find or load critical module '{module_name}'. Check the repo, as it may be defunct or not work with Python {sys.version_info}.")

try:
    from config import HOST, USER, PORT, PASSWORD, MYSQL_SCRIPT_FILE_PATH, DATABASE_NAME, MYSQL_SCRIPT_FILE_PATH
    from logger import Logger
except ImportError as e:
    missing_module = str(e).split("'")[1]
    if missing_module == 'config':
        raise ImportError("Failed to import from config. Make sure the config.py file exists and contains the necessary constants: HOST, USER, PORT, PASSWORD, MYSQL_SCRIPT_FILE_PATH, DATABASE_NAME") from e
    elif missing_module == 'logger':
        raise ImportError("Failed to import Logger. Make sure the logger.py file exists and contains the Logger class") from e
    else:
        raise ImportError(f"Failed to import {missing_module}. Check if it's correctly defined in the respective module") from e

log_level = 10
logger = Logger(logger_name=__name__, log_level=log_level)

SOCIALTOOLKIT_TABLE_NAMES = [
    "locations", # Table of every incorporated village, town, city, and county in the US, along with geolocation data and domains.
    "doc_content", # Table of cleaned documents to be fed to an LLM
    "doc_metadata", # Table of metadata about the documents in doc_content
    "domains", # Table of domains for specific locations (e.g. www.cityofnewyork.gov) and associated metadata. NOTE This is the seed dataset that will be fed to the web crawler.
    "urls", # Table of crawled urls and their associated data. Domains are included, as they may have information to extract. NOTE This is where web crawler output goes.
    "output" # Output of the LLM pipeline.
    "ia_url_metadata", # Metadata of urls saved to the Internet Archive.
    "municode_links" # List of municode library links for every city and county in the Municode Library.
]

class SafeFormatter(string.Formatter):
    def get_value(self, key, args, kwargs):
        if isinstance(key, str):
            return kwargs.get(key, "{" + key + "}")
        else:
            return super().get_value(key, args, kwargs)

    def parse(self, format_string):
        try:
            return super().parse(format_string)
        except ValueError:
            return [(format_string, None, None, None)]

def safe_format(format_string, *args, **kwargs):
    formatter = SafeFormatter()
    return formatter.format(format_string, *args, **kwargs)


class MySqlDatabase:
    """
    Interact directly with a MySQL server via SQL commands and files.\n
    Supports asynchronous query (e.g. SELECT) and alteration commands (e.g. UPDATE, DELETE, INSERT, ALTER, etc).\n
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
    - _get_connection_from_pool(): Get a connection from the pool.
    - _return_connection_to_pool(): Return a connection to the pool.
    - _execute_sql_command(): Execute a SQL command.

    #### External (Async)
    - connect_to_server(): Startup the connections pool to the server.
    - execute_sql_command(): Execute a SQL command via _execute_sql_command.
    - execute_sql_file(): Directly execute a SQL file via _execute_sql_command.
    - query_database(): Execute a simple SELECT query from the MySQL database.
    - alter_database(): Alter the MySQL database.
    - close_connection_to_server(): Close the connections pool.

    ### Example Usage
    >>> async with await MySqlDatabase(database="socialtoolkit") as db\n
    >>>     if query:
    >>>         return await db.execute_sql_command("SELECT * FROM links")\n
    >>>     else:
    >>>         await db.execute_sql_command("UPDATE links SET url = NULL WHERE ping_status = '404'")\n
    """

    def __init__(self, 
                 database: str="socialtoolkit", 
                 pool_name: str="connection_pool", 
                 pool_size: int=5,
                 host: str=HOST,
                 user: str=USER,
                 port: int=PORT,
                 password: str=PASSWORD,
                 num: int=1
                ):
        self.db_config = {
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

        self.mysql_scripts_file_path = MYSQL_SCRIPT_FILE_PATH
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.pool_minsize = pool_size
        self.pool_maxsize = pool_size
        self.pool = None


    async def __aenter__(self) -> 'MySqlDatabase':
        """
        Asynchronous context manager entry method.
        This method is called when entering the `async with` block.
        Equivalent to db = MySqlDatabase().connect_to_server()
        """
        await self._create_pool()  # Create the connection pool asynchronously
        return self  # Return the instance to be used within the `async with` block


    async def __aexit__(self, exc_type, exc_value, traceback) -> None: 
                            # exc_type == Type of Exception, 
                            # exc_value == Value of Exception
        """
        Asynchronous context manager exit method.
        This method is called when exiting the `async with` block.
        Equivalent to db.close_connection_to_server()
        """
        await self.close_connection_to_server()


    @classmethod
    async def connect_to_server(cls) -> 'MySqlDatabase':
        """
        Factory method to startup the connection to the server.
        This function is called/assigned on its own and cannot be used to create an `async with` block.
        """
        instance = cls()
        # Perform async initialization
        await instance._create_pool()
        return instance


    async def _create_pool(self) -> None:
        """
        Create a pool of connections to the MySQL server.
        This reduces server overhead and helps prevent deadlocks caused by async code.

        ### Exceptions
        - ConnectionError: If there's any sort of error creating the pool.
        """
        try:
            logger.debug("Attempting to create MySQL server connection pool...")
            # self.pool = mysql.connector.aio.pooling.MySQLConnectionPool(
            #                 pool_name=self._pool_name,
            #                 pool_size=self._pool_size,
            #                 **self.db_config
            #             )
            self.pool = await aiomysql.create_pool(
                minsize =self.pool_maxsize,
                maxsize =self.pool_maxsize,
                loop = asyncio.get_event_loop(),
                host = self.db_config['host'],
                user = self.db_config['user'],
                port = self.db_config['port'],
                password = self.db_config['password'],
                db = self.db_config['database']
            )
            logger.debug("MySQL server connection pool was successfully created.")
            # logger.debug(f"self.pool type: {type(self.pool)}")
        except Exception as e:
            logger.error(f"Error creating MySQL server connection pool: {e}")
            traceback.print_exc()
            raise ConnectionError("Failed to create MySQL server connection pool.") from e

    async def _get_connection_from_pool(self) -> aiomysql.connection.Connection:
        """
        Get a connection from the connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error getting a connection from the pool.
        """
        try:
            # return await self.pool.get_connection()
            return await self.pool.acquire()
        except mysql.connector.pooling.PoolError as e:
            logger.exception(f"Failed to retrieve connection from the pool: {e}")
            traceback.print_exc()
            raise ConnectionError(f"No available connections in the pool: {e}") from e


    def _return_connection_to_pool(self, 
                                   connection: aiomysql.connection.Connection
                                  ) -> None:
        """
        Return a connection to the connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error returning the connection.
        """
        if not connection.closed:
            logger.debug("Returning connection to pool...")
            self.pool.release(connection)
            logger.debug("Connection returned to pool.")
            return
        else:
            logger.info("Connection already closed. Releasing anyways as test...")
            try:
                self.pool.release(connection)
                return
            except Exception as e:
                logger.exception(f"Failed to release connection: {e}")
                raise ConnectionError(f"Failed to release connection: {e}") from e

    async def close_connection_to_server(self) -> None:
        """
        Close the connection pool.

        ### Exceptions
        - ConnectionError: If there's any sort of error closing the pool.
        """
        try:
            logger.debug(f"Attempting to close connection pool...")
            self.pool.close()
            logger.debug(f"Connection pool closed. Waiting for full closure...")
            await self.pool.wait_closed()
            logger.info("Connection pool fully closed successfully.")
            return

        except Exception as e:
            logger.exception(f"Failed to close the connection pool: {e}")
            traceback.print_exc()
            raise ConnectionError(f"Failed to close the connection pool: {e}") from e


    async def _execute_sql_command(self, 
                                   command: LiteralString, 
                                   params: ( Tuple[Any,...] | List[Tuple[Any,...]] )=None,
                                   connection: aiomysql.Connection=None,
                                   is_query: bool=False,
                                   safe_format_vars: dict=None,
                                   unbuffered: bool=False,
                                   return_dict: bool=False,
                                   size: int=None
                                  ) -> List[Tuple[Any,...]] | List[Dict[str,Any]] | aiomysql.Cursor | None:
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
        - safe_format_vars: Variables for safe string formatting of the SQL command.
        - unbuffered: Whether to use an unbuffered cursor.
        - return_dict: Whether to return results as dictionaries instead of tuples.
        - size: The number of rows to fetch for buffered queries. If default or None, fetches all rows.

        ### Returns
        - List[Tuple[Any,...]]: For buffered queries, a list of tuples containing the query results.
        - Dict[Tuple[Any,...]]: For buffered queries with return_dict=True, a list of dictionaries containing the query results.
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
        - The method automatically adjusts the cursor type based on the unbuffered and return_dict parameters.
        - Error handling includes logging, connection cleanup, and re-raising of exceptions.
        """

        # Value and Type checks.
        if not connection:
            logger.error("Database connection was not provided.")
            raise ValueError("Database connection was not provided.")

        if not command:
            logger.error("No SQL statement provided.")
            self._return_connection_to_pool(connection)
            raise ValueError("No SQL statement provided.")

        # TODO Fix type-checking for LiteralString. Right now, "command" is being interpreted as just a string.
        # if typing.get_origin(command) is not LiteralString:
        #     logger.error(f"'command' must be type LiteralString. It is currently type '{type(command)}'")
        #     raise TypeError(f"'command' must be type LiteralString. It is currently type '{type(command)}'")

        if params:
            if isinstance(params, tuple):
                is_tuple = True
            elif isinstance(params, list) and all(isinstance(item, tuple) for item in params):
                is_tuple = False
            else:
                self._return_connection_to_pool(connection)
                logger.error("Argument 'params' must be type tuple or list[tuple]")
                raise TypeError("Argument 'params' must be type tuple or list[tuple]")

        if safe_format_vars:
            for key, value in safe_format_vars.items():
                if isinstance(value, str):
                    safe_format_vars[key] = connection.escape_string(value)
                else:
                    safe_format_vars[key] = connection.escape(value)
            command = safe_format(command, **safe_format_vars)

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
                            return results if isinstance(results[0], dict) else list(results)

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


    async def _execute_unbuffered_query(self,
                                    command: LiteralString,
                                    params: (Tuple[Any,...] | List[Tuple[Any,...]]) = None,
                                    connection: aiomysql.Connection=None,
                                    is_query:bool=True,
                                    safe_format_vars: dict=None,
                                    unbuffered: bool=True,
                                    return_dict: dict=False,
                                    size: int=None
                                    ) -> AsyncGenerator | None:
        """
        Execute an unbuffered SQL query and yield results asynchronously.

        This method allows for processing large result sets without loading the entire result into memory at once.

        ### Parameters
        - command (LiteralString): The SQL query to execute.
        - params (Tuple[Any,...] | List[Tuple[Any,...]], optional): Parameters to be used with the SQL query.
        - connection (aiomysql.Connection, optional): The database connection to use.
        - is_query (bool, default=True): Indicates if the command is a query (should always be True for this method).
        - safe_format_vars (dict, optional): Variables for safe string formatting of the SQL command.
        - unbuffered (bool, default=True): Indicates if the query should be unbuffered (should always be True for this method).
        - return_dict (bool, default=False): If True, returns each row as a dictionary instead of a tuple.
        - size (int, optional): Not used in this method, but kept for consistency with _execute_sql_command.

        ### Yields
        - row: Each row of the query result. The type depends on the return_dict parameter:
            - If return_dict is True: Dict[str, Any]
            - If return_dict is False: Tuple[Any, ...]

        ### Raises
        - Any exceptions raised by _execute_sql_command or cursor operations.

        ### Note
        This method is a generator that yields results one at a time. It automatically closes the cursor
        and returns the connection to the pool when finished or if an error occurs.
        """
        logger.debug("Executing unbuffered query...")
        try:
            cursor = await self._execute_sql_command(
                command,
                params=params,
                connection=connection,
                is_query=is_query,
                safe_format_vars=safe_format_vars,
                unbuffered=unbuffered,
                return_dict=return_dict,
                size=size
            )
            async for row in cursor:
                yield row
                print(row)
        finally:
            await cursor.close()
            self._return_connection_to_pool(connection)


    async def execute_sql_command(self,
                                  command: LiteralString,
                                  params: ( Tuple[Any,...] | List[Tuple[Any,...]] ) = None,
                                  safe_format_vars: dict=None,
                                  unbuffered:bool=False,
                                  return_dict:bool=False,
                                  size: int=None,
                                 ) -> List[Tuple[Any,...]] | List[Dict[str,Any]] | AsyncGenerator | None:
        """
        Directly execute a SQL command to query or alter the database.

        ### Parameters
        - command: SQL command to execute.
        - params: Parameters/values to feed into the SQL command. Optional
        - safe_format_vars: Variable names and values that are inserted into the command string using the safe_format function. Optional
        - unbuffered: If True, executes an unbuffered query, allowing for processing large result sets without loading the entire result into memory at once.
        - return_dict: If True, returns each row as a dictionary instead of a tuple.
        - size: Number of rows to fetch at a time. If None, fetches all rows.

        ### Returns
        - List[Tuple[Any,...]]: For buffered queries, returns a list of tuples containing the query results.
        - List[Dict[Any]]: For buffered queries with return_dict=True, returns a list of dictionaries containing the query results.
        - AsyncGenerator[List[Tuple[Any,...]] | List[Dict[Any]]]: For unbuffered queries, returns an async generator that yields results one at a time.
        - None: For database alteration commands (non-queries), returns None.

        ### Raises
        - Any exceptions raised by the underlying database operations.

        ### Note
        This method automatically detects whether the command is a query or an alteration command based on its first word.
        It handles both buffered and unbuffered queries, as well as database alterations.
        """

        connection = await self._get_connection_from_pool()

        pattern = re.compile(r"^(SELECT|SHOW|WITH|EXPLAIN)\b", re.IGNORECASE) # If the command starts with SELECT, SHOW, WITH, or EXPLAIN, it's a query.
        is_query = bool(pattern.match(command.strip()))


        # Execute the SQL command.
        if is_query: # Query route
            if unbuffered: # Unbuffered query
                logger.debug(f"Chose unbuffered query route.")
                return await self._execute_unbuffered_query(command, params=params,
                                                        connection=connection,
                                                        is_query=is_query,
                                                        safe_format_vars=safe_format_vars,
                                                        return_dict=return_dict,
                                                        unbuffered=unbuffered,
                                                        size=size)
            else: # Buffered query
                logger.debug(f"Chose buffered query route.")
                results = await self._execute_sql_command(command, params=params,
                                                        connection=connection,
                                                        is_query=is_query,
                                                        safe_format_vars=safe_format_vars,
                                                        return_dict=return_dict,
                                                        size=size)
                self._return_connection_to_pool(connection)
                return results

        else: # Alteration route
            await self._execute_sql_command(command,
                                            params=params,
                                            connection=connection,
                                            safe_format_vars=safe_format_vars)
            self._return_connection_to_pool(connection)
            return


    async def query_to_dataframe(self, 
                                 query: str, 
                                 params: tuple = None, 
                                 safe_format_vars: dict = None, 
                                 unbuffered=False
                                 ) -> pd.DataFrame:
        """
        Execute a MySQL query and return the results as a Pandas DataFrame.

        ### Parameters
        - query (str): The SQL query to execute.
        - params (tuple, optional): Parameters for the SQL query.
        - unbuffered: If True, executes an unbuffered query, allowing for processing large result sets without loading the entire result into memory at once.

        ### Returns
        - A Pandas DataFrame of the query results.
        """
        results = await self.execute_sql_command(query, 
                                                 params=params, 
                                                 unbuffered=unbuffered,
                                                 safe_format_vars=safe_format_vars,
                                                 return_dict=True)
        return pd.DataFrame.from_dict(results)

