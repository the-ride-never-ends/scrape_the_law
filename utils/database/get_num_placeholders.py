

def get_num_placeholders(args: dict|list) -> str:
    placeholders = ", ".join(["%s"] * len(args))
    return placeholders

