import requests
import csv
import time
import os
import tarfile
import pandas as pd
from tqdm import tqdm


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Modify these paths when moving the script to another location

# Temporary download and extraction paths
DUMP_PATH = "db-dump.tar.gz"
EXTRACT_PATH = "crates-db"

# Output path: Location where the CSV file will be saved
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'package-lists'))
OUTPUT_FILENAME = "Crates.csv"

# ============================================================================

def download_file(url, filename):
    """Downloads a file from a URL with a progress bar."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 Kilobyte
    
    with open(filename, "wb") as f, tqdm(
        desc=filename,
        total=total_size,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(block_size):
            bar.update(len(data))
            f.write(data)

def mine_crates():
    """Mines crates.io to get the whole list of Rust packages from the database dump."""
    
    dump_url = "https://static.crates.io/db-dump.tar.gz"
    dump_path = DUMP_PATH
    extract_path = EXTRACT_PATH

    # Download the database dump
    if not os.path.exists(dump_path):
        print("Downloading crates.io database dump...")
        download_file(dump_url, dump_path)
    else:
        print("Database dump already downloaded.")

    # Extract the database dump
    print("Extracting database dump...")
    with tarfile.open(dump_path, "r:gz") as tar:
        tar.extractall(path=extract_path)

    # Delete the tar.gz file
    if os.path.exists(dump_path):
        print("Deleting database dump archive...")
        os.remove(dump_path)
    
    # Find the actual data directory (it has a date in the name)
    data_dir = ""
    for item in os.listdir(extract_path):
        if os.path.isdir(os.path.join(extract_path, item)):
            data_dir = os.path.join(extract_path, item)
            break
    
    if not data_dir:
        print("Could not find data directory in the extracted archive.")
        return

    crates_csv_path = os.path.join(data_dir, "data", "crates.csv")
    if not os.path.exists(crates_csv_path):
        print(f"crates.csv not found in {data_dir}")
        return

    print("Processing crate data...")
    # Read the crates data
    df = pd.read_csv(crates_csv_path)

    # Create the path to the output file
    output_dir = OUTPUT_DIR
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, OUTPUT_FILENAME)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Platform", "Name", "Homepage URL", "Repository URL"])

        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Writing to CSV"):
            writer.writerow([
                index + 1,
                "Crates.io",
                row["name"],
                row["homepage"],
                row["repository"],
            ])

    print(f"Successfully saved {df.shape[0]} crates to {output_file}")

if __name__ == "__main__":
    mine_crates()
