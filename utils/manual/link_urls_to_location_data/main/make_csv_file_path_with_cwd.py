import os

def make_csv_file_path_with_cwd(source: str) -> str:
    """
    
    Examples:
    >>> return make_csv_file_path_with_cwd("general_code")
    'path/to/file/general_code_results.csv'
    """
    return os.path.join(os.getcwd(), f"{source}_results.csv")
