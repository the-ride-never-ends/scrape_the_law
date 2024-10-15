
from typing import Iterable

from .get_column_names import get_column_names

def make_update_on_duplicate_key_clause(inp: Iterable) -> str:
    """
    Create a MySQL upsert clause for handling duplicate key conflicts.

    This function generates the 'ON DUPLICATE KEY UPDATE' part of a MySQL
    upsert statement. It creates a clause that updates all columns with their
    corresponding values when a duplicate key is encountered during an insert.

    NOTE: This assumes that the table being inserted into has a unique key of some kind.

    Args:
        inp (Iterable): An iterable containing the input data. The column names
                        will be extracted from this input.

    Returns:
        str: A string containing the MySQL upsert clause.

    Example:
        >>> make_update_on_duplicate_key_clause(['id', 'name', 'age'])
        ' ON DUPLICATE KEY UPDATE id = VALUES(id), name = VALUES(name), age = VALUES(age);'
    """

    clause = ''
    clause += 'ON DUPLICATE KEY UPDATE '
    clause += ', '.join(f'{name} = VALUES({name})' for name in get_column_names(inp, return_str_list=True))
    clause += ';'
    
    return clause
# def create_mysql_update_clause(input_list, condition):
#     if not input_list:
#         return ''
    
#     clause = 'UPDATE table_name SET '
#     for i, item in enumerate(input_list):
#         clause += f'column{i+1} = %s'
#         if i < len(input_list) - 1:
#             clause += ', '
    
#     clause += f' WHERE {condition}'
#     return clause
