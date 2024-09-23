import re

def format_sql_file(filepath: str) -> str|list[str]:
    """
    Read and format an SQL file, removing comments and splitting into individual commands.

    Args:
        filepath (str): The path to the SQL file.

    Returns:
        list[str]: A list of individual SQL commands.
    """
    with open(filepath, 'r') as file:
        sql_statement = file.read()

    # Remove SQL comments (anything between -- and end of line)
    sql_statement = re.sub(r'--.*?(\n|$)', '\n', sql_statement)

    # all SQL commands (split on ';')
    sql_commands = sql_statement.split(';')

    return sql_commands