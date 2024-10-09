

def get_column_names(args: dict|list[str]) -> str:
    columns = ", ".join(args.keys()) if isinstance(args, dict) else ", ".join(args)
    return columns

