import csv

from . import read_domain_csv, reconstruct_domain_from_csv_filename

def read_domain_csv(file_path: str, skip_headers: bool=True) -> list[tuple]:
    output = []
    try:
        domain = reconstruct_domain_from_csv_filename(file_path)
        with open(file_path, mode='r', newline='') as csvfile:
            reader = csv.reader(csvfile)

            if skip_headers:
                next(reader, None)

            for row in reader:
                if row:  # Ensure the row is not empty
                    row = list(row)
                    row[1] = read_domain_csv(row[1]) # Convert the timestamp to a string.
                    row.append(domain)
                    row = tuple(row)
                    output.append(row)

    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return output
