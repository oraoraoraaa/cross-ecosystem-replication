import requests
import csv
import os
import json
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Modify these paths when moving the script to another location

# Output path: Location where the CSV file will be saved
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'package-lists'))
OUTPUT_FILENAME = "NPM.csv"

# Checkpoint files
CHECKPOINT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '.checkpoint'))
PACKAGE_NAMES_FILE = os.path.join(CHECKPOINT_DIR, "package_names.txt")  # Store names line by line
INDEX_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "index_checkpoint.json")

# ============================================================================

def fetch_with_retry(url, params=None, timeout=300, max_retries=5, backoff_factor=2):
    """
    Fetch URL with exponential backoff retry logic.
    
    Args:
        url: The URL to fetch
        params: Query parameters
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor for exponential backoff
    
    Returns:
        Response object if successful, None otherwise
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout, stream=False)
            
            # For 429 (rate limit), wait longer before retrying
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = 60  # Wait 1 minute for rate limits
                    print(f"  Rate limited (attempt {attempt + 1}/{max_retries})")
                    print(f"  Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return response
            
            # For other 4xx errors (client errors like 404), don't retry - just return the response
            # The caller can check the status code
            if 400 <= response.status_code < 500:
                return response
            
            # For 5xx errors (server errors), raise and retry
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
                # Return None instead of raising to allow processing to continue
                return None
        except requests.exceptions.HTTPError as e:
            # Server errors (5xx) - retry
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                print(f"  Server error (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {max_retries} attempts: {e}")
                # Return None instead of raising to allow processing to continue
                return None
        except requests.exceptions.RequestException as e:
            print(f"  Request error: {str(e)[:100]}")
            # Return None instead of raising to allow processing to continue
            return None
    return None

def save_index_checkpoint(last_key, total_rows, total_packages):
    """Save index download checkpoint to disk."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    checkpoint_data = {
        'last_key': last_key,
        'total_rows': total_rows,
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
                return data.get('last_key'), data.get('total_rows', 0), data.get('total_packages', 0)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load checkpoint: {e}")
            return None, 0, 0
    return None, 0, 0

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
        # Skip to start index
        for _ in range(start_idx):
            next(f, None)
        
        # Read batch
        for i, line in enumerate(f):
            if i >= batch_size:
                break
            packages.append(line.strip())
    
    return packages

def save_package_names_checkpoint(package_names, last_key, total_rows):
    """Save package names checkpoint - only saves new packages to file."""
    append_package_names(package_names)
    total_packages = get_package_names_count()
    save_index_checkpoint(last_key, total_rows, total_packages)
    print(f"  Checkpoint saved: {total_packages:,} packages total")

def load_package_names_checkpoint():
    """Load package names checkpoint - returns metadata only."""
    last_key, total_rows, total_packages = load_index_checkpoint()
    if total_packages > 0:
        print(f"Found checkpoint with {total_packages:,} packages")
        print(f"Last key: {last_key if last_key else 'N/A'}")
        return True, last_key, total_rows, total_packages
    return False, None, 0, 0

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
                "NPM",
                package_name,
                homepage_url,
                repo_url,
            ])
        f.flush()  # Ensure data is written to disk
        os.fsync(f.fileno())  # Force write to disk

def mine_npm_packages():
    """Mines npm registry to get the whole list of npm packages."""
    
    print("Fetching npm package list from registry...")
    print("This will download the complete package database with pagination")
    
    # Try to load checkpoint
    print("Checking for existing checkpoint...")
    has_checkpoint, checkpoint_last_key, checkpoint_total, checkpoint_count = load_package_names_checkpoint()
    
    # npm provides an all-docs endpoint that returns all package names
    # Use startkey parameter for pagination (CouchDB-style)
    base_url = "https://replicate.npmjs.com/_all_docs"
    
    # Initialize with checkpoint data if available
    if has_checkpoint:
        print(f"Resuming from checkpoint with {checkpoint_count:,} packages")
        startkey = checkpoint_last_key
        total_rows = checkpoint_total
        batch_count = checkpoint_count // 10000
        
        # If we have a complete list, skip batch fetching
        if checkpoint_count >= total_rows * 0.99:  # Allow 1% margin
            print("Package name collection appears complete, skipping to detail fetching...")
            skip_fetch = True
        else:
            print(f"Will resume from key: {startkey}")
            skip_fetch = False
    else:
        startkey = None
        batch_count = 0
        total_rows = 0
        checkpoint_count = 0
        skip_fetch = False
    
    limit = 10000  # Maximum allowed by the API
    
    # Track seen packages in current session (to avoid duplicates within session)
    # This is cleared periodically to keep memory usage low
    seen_in_session = set()
    
    try:
        # Only fetch batches if we don't have a complete checkpoint
        if not skip_fetch:
            print("Downloading package names from npm registry (paginated)...")
            print("Note: Progress is saved after each batch for recovery")
            
            # Fetch first batch if not resuming
            if not has_checkpoint:
                batch_count = 1
                print(f"  Fetching batch {batch_count}...")
                response = fetch_with_retry(base_url, params={'limit': limit}, timeout=300)
                response.raise_for_status()
                data = response.json()
                
                rows = data.get('rows', [])
                total_rows = data.get('total_rows', 0)
                
                print(f"  Total packages in registry: {total_rows:,}")
                
                batch_packages = []
                for row in rows:
                    pkg_id = row['id']
                    if not pkg_id.startswith('_design/'):
                        batch_packages.append(pkg_id)
                        seen_in_session.add(pkg_id)
                
                print(f"  Got {len(batch_packages)} packages from first batch")
                
                # Save initial checkpoint
                save_package_names_checkpoint(batch_packages, batch_packages[-1] if batch_packages else None, total_rows)
            else:
                # When resuming, we need to fetch to get the current state
                rows = [{'id': startkey}] * limit  # Dummy to enter the loop
            
            # Continue with pagination using startkey
            while len(rows) >= limit:
                batch_count += 1
                last_key = rows[-1]['id']
                
                # Use startkey with the last key we got
                params = {
                    'limit': limit,
                    'startkey': json.dumps(last_key)
                }
                
                print(f"  Fetching batch {batch_count} starting from '{last_key[:50]}...'")
                response = fetch_with_retry(base_url, params=params, timeout=300)
                response.raise_for_status()
                data = response.json()
                
                rows = data.get('rows', [])
                if not rows:
                    break
                
                batch_packages = []
                new_count = 0
                for row in rows:
                    pkg_id = row['id']
                    # Skip first item if it matches our startkey (it's a duplicate)
                    if pkg_id == last_key:
                        continue
                    if not pkg_id.startswith('_design/') and pkg_id not in seen_in_session:
                        batch_packages.append(pkg_id)
                        seen_in_session.add(pkg_id)
                        new_count += 1
                
                # Clear session cache periodically to save memory
                if len(seen_in_session) > 50000:
                    seen_in_session.clear()
                
                current_total = get_package_names_count()
                print(f"  Got {new_count} new packages")
                print(f"  Total unique packages collected: {current_total:,} / {total_rows:,}")
                
                # Save checkpoint every batch
                save_package_names_checkpoint(batch_packages, last_key, total_rows)
                
                # If we didn't get any new packages, stop
                if new_count == 0:
                    print("  No new packages found, stopping pagination")
                    break
        
        total_packages = get_package_names_count()
        print(f"Found {total_packages:,} npm packages in total")
        
        # Save final checkpoint
        _, last_key, total_rows, _ = load_package_names_checkpoint()
        save_index_checkpoint(last_key, total_rows, total_packages)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading npm package names: {e}")
        print("Progress has been saved. You can resume by running the script again.")
        return
    
    # Create the path to the output file
    output_dir = OUTPUT_DIR
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, OUTPUT_FILENAME)
    
    # Load already processed packages
    print("Checking for existing results...")
    processed_results = load_processed_packages(output_file)
    
    # Get total package count
    total_packages = get_package_names_count()
    
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
        print("Done!")
        return
    
    print(f"Total packages to process: {total_to_process:,}")
    print(f"Already processed: {len(processed_results):,} packages")
    print("Using parallel processing with 20 workers to avoid overwhelming the registry...")
    print("Processing in batches of 1000 packages to conserve memory...")
    print("Progress is saved after each batch for recovery")
    
    def fetch_package_info(package_name):
        """Fetch information for a single npm package."""
        package_info_url = f"https://registry.npmjs.org/{package_name}"
        
        homepage_url = "nan"
        repo_url = "nan"
        
        try:
            # Add small delay to avoid overwhelming the server
            time.sleep(0.05)  # 50ms delay between requests
            
            response = fetch_with_retry(package_info_url, timeout=15, max_retries=5, backoff_factor=2)
            if response and response.status_code == 200:
                package_info = response.json()
                
                # Get homepage URL
                homepage_url = package_info.get('homepage', 'nan')
                if not homepage_url or homepage_url == '':
                    homepage_url = "nan"
                elif not homepage_url.startswith('http'):
                    homepage_url = "nan"
                
                # Get repository URL
                repo_info = package_info.get('repository', {})
                if isinstance(repo_info, dict):
                    repo_url = repo_info.get('url', 'nan')
                elif isinstance(repo_info, str):
                    repo_url = repo_info
                else:
                    repo_url = "nan"
                
                # Keep URLs as-is from the registry (no normalization)
                # Package-Filter will handle normalization
                if not repo_url or repo_url == "nan" or repo_url == "":
                    repo_url = "nan"
                    
        except (requests.exceptions.RequestException, ValueError, KeyError):
            # If API call fails, continue with nan values
            pass
        
        return package_name, homepage_url, repo_url
    
    # Process packages in batches of 1000 to conserve memory
    batch_size = 1000
    start_idx = 0
    processed_count = 0
    
    # If starting fresh, write header; otherwise append
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
                # Submit all tasks for packages in this batch
                future_to_package = {
                    executor.submit(fetch_package_info, package_name): (idx, package_name)
                    for idx, package_name in packages_to_process_batch
                }
                
                # Process completed tasks with progress bar
                for future in tqdm(as_completed(future_to_package), total=len(future_to_package), desc=f"Batch {start_idx // batch_size + 1}"):
                    idx, original_name = future_to_package[future]
                    try:
                        package_name, homepage_url, repo_url = future.result()
                        new_results[idx] = (package_name, homepage_url, repo_url)
                    except Exception as e:
                        # If something went wrong, store with nan values
                        new_results[idx] = (original_name, "nan", "nan")
                    
                    processed_count += 1
            
            # Save batch results to CSV immediately
            if new_results:
                print(f"  Saving {len(new_results)} packages to CSV...")
                save_results_incrementally(output_file, new_results, write_header)
                # Verify the save by checking file exists and size
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    print(f"  ✓ Saved to {output_file} (size: {file_size:,} bytes)")
                else:
                    print(f"  ✗ Warning: Output file not found after save!")
                write_header = False  # Don't write header again
            
            # Move to next batch
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
    mine_npm_packages()
