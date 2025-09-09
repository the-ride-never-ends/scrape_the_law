import os

# EXAMPLE OF os.walk
# os.walk returns a generator, that creates a tuple of values (current_path, directories in current_path, files in current_path).
#
# Every time the generator is called it will follow each directory recursively until no further sub-directories are available from the initial directory that walk was called upon.
#
# As such,
#
# os.walk('C:\dir1\dir2\startdir').next()[0] # returns 'C:\dir1\dir2\startdir'
# os.walk('C:\dir1\dir2\startdir').next()[1] # returns all the dirs in 'C:\dir1\dir2\startdir'
# os.walk('C:\dir1\dir2\startdir').next()[2] # returns all the files in 'C:\dir1\dir2\startdir'


def delete_empty_log_files(root_folder):
    """
    Delete all empty log files recursively from a root folder.
    
    Walks through a directory tree starting from root_folder and removes any
    .log files that are 0 bytes in size. Prints notification for each deleted file.
    
    Args:
        root_folder (str): Path to the root directory to search for empty log files.
    
    Returns:
        None: This function performs file deletion side effects.
    
    Raises:
        OSError: If file deletion fails due to permissions or file system errors.
        FileNotFoundError: If root_folder doesn't exist.
    
    Example:
        >>> delete_empty_log_files('/path/to/logs')
        # Deletes any empty .log files and prints:
        # Deleted empty file: /path/to/logs/empty.log
    """
    for root, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith('.log'):
                file_path = os.path.join(root, filename)
                if os.path.getsize(file_path) == 0: # 0kb
                    os.remove(file_path)
                    print(f"Deleted empty file: {file_path}")


def delete_zone_identifier_files(root_folder):
    """
    Delete all empty Zone.Identifier files recursively from a root folder.
    
    Walks through a directory tree starting from root_folder and removes any
    files ending with '.Identifier' that are 0 bytes in size. These are typically
    Windows security zone identifier files created when downloading files.
    
    Args:
        root_folder (str): Path to the root directory to search for Zone.Identifier files.
    
    Returns:
        None: This function performs file deletion side effects.
    
    Raises:
        OSError: If file deletion fails due to permissions or file system errors.
        FileNotFoundError: If root_folder doesn't exist.
    
    Example:
        >>> delete_zone_identifier_files('/path/to/files')
        # Deletes any empty .Identifier files and prints:
        # Deleted empty file: /path/to/files/file.txt:Zone.Identifier
    """
    for root, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith('.Identifier'):
                file_path = os.path.join(root, filename)
                if os.path.getsize(file_path) == 0: # 0kb
                    os.remove(file_path)
                    print(f"Deleted empty file: {file_path}")


