import os

def reconstruct_domain_from_csv_filename(file_path: str, split_extension: bool=True ) -> str:
    """
    Reconstruct a domain from a waybackup csv filename 
    """
    # Extract the filename from the filepath
    filename = os.path.basename(file_path)

    if split_extension:
        filename, _ = os.path.splitext(filename)

    # Remove 'waybackup_' prefix
    domain = filename.replace('waybackup_', '', 1)

    # Remove 'http.' or 'https.' if present
    domain = domain.replace('http.', '', 1).replace('https.', '', 1)

    # Remove 'www.' if present
    domain = domain.replace('www.', '', 1)

    return domain