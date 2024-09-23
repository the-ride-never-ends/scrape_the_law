
from .get_num_placeholders import get_num_placeholders
from .get_column_names import get_column_names

def get_insert_into_values(args: dict|list[str]) -> tuple[str,str]:
    return get_column_names(args), get_num_placeholders(args)
