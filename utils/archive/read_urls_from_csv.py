import csv

def read_urls_from_csv(file_path: str) -> list:
    """
    Read URLs from a CSV file (first column)
    """
    urls = []
    try:
        with open(file_path, mode='r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:  # Ensure the row is not empty
                    urls.append(row[0])  # Assuming the URL is in the first column
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return urls