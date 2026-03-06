import requests
import csv
import os
import gzip
import json
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Modify these paths when moving the script to another location

# Output path: Location where the CSV file will be saved
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'package-lists'))
OUTPUT_FILENAME = "PHP.csv"

# Checkpoint files
CHECKPOINT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '.checkpoint'))
PACKAGE_NAMES_FILE = os.path.join(CHECKPOINT_DIR, "package_names.txt")  # Store names line by line
INDEX_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "index_checkpoint.json")

# ============================================================================

def create_session():
    """Creates a requests session with connection pooling and retries."""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=40
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    session.headers.update({
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'PHP-Miner/1.0'
    })
    
    return session

def fetch_with_retry(url, params=None, timeout=300, max_retries=5, backoff_factor=2):
    """
    Fetch URL with exponential backoff retry logic.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout, stream=False)
            
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = 60
                    print(f"  Rate limited (attempt {attempt + 1}/{max_retries})")
                    print(f"  Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return response
            
            if 400 <= response.status_code < 500:
                return response
            
            response.raise_for_status()
            return response
        except (requests.exceptions.ChunkedEncodingError, 
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.SSLError) as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                print(f"  Connection error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {max_retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.HTTPError as e:
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                print(f"  Server error (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {max_retries} attempts: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {str(e)[:100]}")
            return None
    return None

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

def save_index_checkpoint(total_packages):
    """Save index download checkpoint to disk."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    checkpoint_data = {
        'total_packages': total_packages,
        'timestamp': time.time()
    }
    with open(INDEX_CHECKPOINT, 'w', encoding='utf-8') as f:
        json.dump(checkpoint_data, f)

def load_index_checkpoint():
    """Load index download checkpoint from disk."""
    if os.path.exists(INDEX_CHECKPOINT):
        try:
            with open(INDEX_CHECKPOINT, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('total_packages', 0)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load checkpoint: {e}")
            return 0
    return 0

def append_package_names(package_names):
    """Append package names to file (memory efficient)."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    with open(PACKAGE_NAMES_FILE, 'a', encoding='utf-8') as f:
        for name in package_names:
            f.write(name + '\n')

def get_package_names_count():
    """Get total count of package names without loading all into memory."""
    if not os.path.exists(PACKAGE_NAMES_FILE):
        return 0
    count = 0
    with open(PACKAGE_NAMES_FILE, 'r', encoding='utf-8') as f:
        for _ in f:
            count += 1
    return count

def load_package_names_batch(start_idx, batch_size=1000):
    """Load a batch of package names from file."""
    if not os.path.exists(PACKAGE_NAMES_FILE):
        return []
    
    packages = []
    with open(PACKAGE_NAMES_FILE, 'r', encoding='utf-8') as f:
        for _ in range(start_idx):
            next(f, None)
        
        for i, line in enumerate(f):
            if i >= batch_size:
                break
            packages.append(line.strip())
    
    return packages

def save_package_names_checkpoint(package_names):
    """Save package names checkpoint - saves new packages to file."""
    # Clear existing file and write all packages
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    with open(PACKAGE_NAMES_FILE, 'w', encoding='utf-8') as f:
        for name in package_names:
            f.write(name + '\n')
    total_packages = len(package_names)
    save_index_checkpoint(total_packages)
    print(f"  Checkpoint saved: {total_packages:,} packages total")

def load_package_names_checkpoint():
    """Load package names checkpoint - returns metadata only."""
    total_packages = load_index_checkpoint()
    if total_packages > 0 and os.path.exists(PACKAGE_NAMES_FILE):
        actual_count = get_package_names_count()
        print(f"Found checkpoint with {actual_count:,} packages")
        return True, actual_count
    return False, 0

def load_processed_packages(output_file):
    """Load already processed packages from the CSV file."""
    processed = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    idx = int(row['ID'])
                    processed[idx] = (row['Name'], row['Homepage URL'], row['Repository URL'])
            print(f"Found {len(processed)} already processed packages")
        except (csv.Error, KeyError, ValueError) as e:
            print(f"Warning: Failed to load existing CSV: {e}")
            return {}
    return processed

def save_results_incrementally(output_file, results, write_header=False):
    """Save results to CSV incrementally."""
    mode = 'w' if write_header else 'a'
    with open(output_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["ID", "Platform", "Name", "Homepage URL", "Repository URL"])
        
        for idx in sorted(results.keys()):
            package_name, homepage_url, repo_url = results[idx]
            writer.writerow([
                idx,
                "Packagist",
                package_name,
                homepage_url,
                repo_url,
            ])
        f.flush()
        os.fsync(f.fileno())

def fetch_package_info(package_name):
    """Fetch information for a single package."""
    package_url = f"https://packagist.org/packages/{package_name}.json"
    
    homepage_url = "nan"
    repo_url = "nan"
    
    try:
        # Add small delay to avoid overwhelming the server
        time.sleep(0.05)  # 50ms delay between requests
        
        response = fetch_with_retry(package_url, timeout=15, max_retries=5, backoff_factor=2)
        if response and response.status_code == 200:
            package_info = response.json()
            
            # Navigate through the JSON structure
            package_data = package_info.get('package', {})
            
            # Get homepage - try multiple sources
            homepage_url = package_data.get('homepage', '') or "nan"
            
            # Get repository URL
            repository = package_data.get('repository', '')
            if repository:
                repo_url = repository
            else:
                # Try to extract from versions
                versions = package_data.get('versions', {})
                if versions:
                    # Get the latest version info
                    for version_key in ['dev-master', 'dev-main', 'master', 'main']:
                        if version_key in versions:
                            version_data = versions[version_key]
                            source = version_data.get('source', {})
                            if source and 'url' in source:
                                repo_url = source['url']
                                break
                    
                    # If still not found, try the first available version
                    if repo_url == "nan" and versions:
                        first_version = next(iter(versions.values()))
                        source = first_version.get('source', {})
                        if source and 'url' in source:
                            repo_url = source['url']
            
            # Clean up URLs
            if homepage_url and homepage_url != "nan" and not homepage_url.startswith('http'):
                homepage_url = "nan"
            if repo_url and repo_url != "nan" and not repo_url.startswith('http'):
                repo_url = "nan"
                
    except (requests.exceptions.RequestException, ValueError, json.JSONDecodeError):
        # If API call fails, continue with nan values
        pass
    
    return package_name, homepage_url, repo_url

def mine_php_packages():
    """Mines Packagist.org to get the whole list of PHP packages."""
    
    # Packagist provides a packages.json file with all package names
    packages_url = "https://packagist.org/packages/list.json"
    
    print("Checking for existing checkpoint...")
    has_checkpoint, checkpoint_count = load_package_names_checkpoint()
    
    if has_checkpoint and checkpoint_count > 0:
        print(f"Resuming from checkpoint with {checkpoint_count:,} packages")
        total_packages = checkpoint_count
    else:
        print("Downloading Packagist package list...")
        
        try:
            response = fetch_with_retry(packages_url, timeout=120)
            if response is None or response.status_code != 200:
                print(f"Error downloading package list")
                return
            
            data = response.json()
            package_names = data.get('packageNames', [])
            print(f"Found {len(package_names)} PHP packages")
            
            # Save package names to checkpoint
            save_package_names_checkpoint(package_names)
            total_packages = len(package_names)
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading package list: {e}")
            return
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return
    
    if total_packages == 0:
        print("No packages found.")
        return
    
    # Create the path to the output file
    output_dir = OUTPUT_DIR
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, OUTPUT_FILENAME)
    
    # Load already processed packages
    print("Checking for existing results...")
    processed_results = load_processed_packages(output_file)
    
    # Determine which packages still need processing
    total_to_process = total_packages - len(processed_results)
    
    if total_to_process <= 0:
        print("All packages already processed!")
        print(f"Results are in {output_file}")
        # Clean up checkpoint files
        print("Cleaning up checkpoint files...")
        if os.path.exists(PACKAGE_NAMES_FILE):
            os.remove(PACKAGE_NAMES_FILE)
        if os.path.exists(INDEX_CHECKPOINT):
            os.remove(INDEX_CHECKPOINT)
        if os.path.exists(CHECKPOINT_DIR):
            try:
                os.rmdir(CHECKPOINT_DIR)
            except OSError:
                pass
        print("Done!")
        return
    
    print(f"Total packages to process: {total_to_process:,}")
    print(f"Already processed: {len(processed_results):,} packages")
    print("Using parallel processing with 20 workers to avoid overwhelming the registry...")
    print("Processing in batches of 1000 packages to conserve memory...")
    print("Progress is saved after each batch for recovery")
    
    # Process packages in batches
    batch_size = 1000
    start_idx = 0
    processed_count = 0
    
    write_header = len(processed_results) == 0
    
    try:
        while start_idx < total_packages:
            # Load batch of package names
            batch_packages = load_package_names_batch(start_idx, batch_size)
            if not batch_packages:
                break
            
            # Create list of packages to process in this batch
            packages_to_process_batch = []
            for i, package_name in enumerate(batch_packages):
                idx = start_idx + i + 1
                if idx not in processed_results:
                    packages_to_process_batch.append((idx, package_name))
            
            if not packages_to_process_batch:
                start_idx += batch_size
                continue
            
            print(f"\nProcessing batch {start_idx // batch_size + 1}: packages {start_idx + 1} to {start_idx + len(batch_packages)}")
            print(f"  {len(packages_to_process_batch)} packages to process in this batch")
            
            new_results = {}
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                future_to_package = {
                    executor.submit(fetch_package_info, package_name): (idx, package_name)
                    for idx, package_name in packages_to_process_batch
                }
                
                for future in tqdm(as_completed(future_to_package), total=len(future_to_package), desc=f"Batch {start_idx // batch_size + 1}"):
                    idx, original_name = future_to_package[future]
                    try:
                        package_name, homepage_url, repo_url = future.result()
                        new_results[idx] = (package_name, homepage_url, repo_url)
                    except Exception as e:
                        new_results[idx] = (original_name, "nan", "nan")
                    
                    processed_count += 1
            
            # Save batch results to CSV immediately
            if new_results:
                print(f"  Saving {len(new_results)} packages to CSV...")
                save_results_incrementally(output_file, new_results, write_header)
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    print(f"  ✓ Saved to {output_file} (size: {file_size:,} bytes)")
                else:
                    print(f"  ✗ Warning: Output file not found after save!")
                write_header = False
            
            start_idx += batch_size
        
        print("\nAll processing complete!")
        print(f"Total packages processed: {processed_count:,}")
        print(f"Successfully saved to {output_file}")
        
        # Clean up checkpoint files
        print("Cleaning up checkpoint files...")
        if os.path.exists(PACKAGE_NAMES_FILE):
            os.remove(PACKAGE_NAMES_FILE)
        if os.path.exists(INDEX_CHECKPOINT):
            os.remove(INDEX_CHECKPOINT)
        if os.path.exists(CHECKPOINT_DIR):
            try:
                os.rmdir(CHECKPOINT_DIR)
            except OSError:
                pass
        print("Done!")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user!")
        print("Progress has been saved. Run the script again to resume.")
        raise
    except Exception as e:
        print(f"\n\nError during processing: {e}")
        print("Progress has been saved. Run the script again to resume.")
        raise

if __name__ == "__main__":
    mine_php_packages()
