import requests
import csv
import os
import json
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
# Modify these paths when moving the script to another location

# Output path: Location where the CSV file will be saved
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'package-lists'))
OUTPUT_FILENAME = "Ruby.csv"

# Checkpoint files
CHECKPOINT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '.checkpoint'))
GEM_NAMES_FILE = os.path.join(CHECKPOINT_DIR, "gem_names.txt")  # Store names line by line
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
        'User-Agent': 'Ruby-Miner/1.0'
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

def save_index_checkpoint(total_gems):
    """Save index download checkpoint to disk."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    checkpoint_data = {
        'total_gems': total_gems,
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
                return data.get('total_gems', 0)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load checkpoint: {e}")
            return 0
    return 0

def append_gem_names(gem_names):
    """Append gem names to file (memory efficient)."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    with open(GEM_NAMES_FILE, 'a', encoding='utf-8') as f:
        for name in gem_names:
            f.write(name + '\n')

def get_gem_names_count():
    """Get total count of gem names without loading all into memory."""
    if not os.path.exists(GEM_NAMES_FILE):
        return 0
    count = 0
    with open(GEM_NAMES_FILE, 'r', encoding='utf-8') as f:
        for _ in f:
            count += 1
    return count

def load_gem_names_batch(start_idx, batch_size=1000):
    """Load a batch of gem names from file."""
    if not os.path.exists(GEM_NAMES_FILE):
        return []
    
    gems = []
    with open(GEM_NAMES_FILE, 'r', encoding='utf-8') as f:
        for _ in range(start_idx):
            next(f, None)
        
        for i, line in enumerate(f):
            if i >= batch_size:
                break
            gems.append(line.strip())
    
    return gems

def save_gem_names_checkpoint(gem_names):
    """Save gem names checkpoint - saves gems to file."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    with open(GEM_NAMES_FILE, 'w', encoding='utf-8') as f:
        for name in gem_names:
            f.write(name + '\n')
    total_gems = len(gem_names)
    save_index_checkpoint(total_gems)
    print(f"  Checkpoint saved: {total_gems:,} gems total")

def load_gem_names_checkpoint():
    """Load gem names checkpoint - returns metadata only."""
    total_gems = load_index_checkpoint()
    if total_gems > 0 and os.path.exists(GEM_NAMES_FILE):
        actual_count = get_gem_names_count()
        print(f"Found checkpoint with {actual_count:,} gems")
        return True, actual_count
    return False, 0

def load_processed_gems(output_file):
    """Load already processed gems from the CSV file."""
    processed = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    idx = int(row['ID'])
                    processed[idx] = (row['Name'], row['Homepage URL'], row['Repository URL'])
            print(f"Found {len(processed)} already processed gems")
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
            gem_name, homepage_url, repo_url = results[idx]
            writer.writerow([
                idx,
                "RubyGems",
                gem_name,
                homepage_url,
                repo_url,
            ])
        f.flush()
        os.fsync(f.fileno())

def fetch_gem_info(gem_name):
    """Fetch information for a single gem."""
    gem_info_url = f"https://rubygems.org/api/v1/gems/{gem_name}.json"
    
    homepage_url = "nan"
    repo_url = "nan"
    
    try:
        # Add small delay to avoid overwhelming the server
        time.sleep(0.05)  # 50ms delay between requests
        
        response = fetch_with_retry(gem_info_url, timeout=15, max_retries=5, backoff_factor=2)
        if response and response.status_code == 200:
            gem_info = response.json()
            homepage_url = gem_info.get('homepage_uri', '') or gem_info.get('project_uri', '') or "nan"
            repo_url = gem_info.get('source_code_uri', '') or gem_info.get('homepage_uri', '') or "nan"
            
            # Clean up URLs
            if homepage_url and homepage_url != "nan" and not homepage_url.startswith('http'):
                homepage_url = "nan"
            if repo_url and repo_url != "nan" and not repo_url.startswith('http'):
                repo_url = "nan"
                
    except (requests.exceptions.RequestException, ValueError):
        # If API call fails, continue with nan values
        pass
    
    return gem_name, homepage_url, repo_url

def mine_ruby_gems():
    """Mines RubyGems.org to get the whole list of Ruby packages."""
    
    # Fetch gem names from RubyGems API
    print("Checking for existing checkpoint...")
    has_checkpoint, checkpoint_count = load_gem_names_checkpoint()
    
    if has_checkpoint and checkpoint_count > 0:
        print(f"Resuming from checkpoint with {checkpoint_count:,} gems")
        total_gems = checkpoint_count
    else:
        print("Fetching gem names from RubyGems API...")
        names_url = "http://rubygems.org/names"
        
        try:
            print("Downloading list of all gem names...")
            response = fetch_with_retry(names_url, timeout=120)
            if response is None or response.status_code != 200:
                print(f"Error downloading gem names")
                return
            
            gem_names = response.text.strip().split('\n')
            print(f"Found {len(gem_names)} gems")
            
            # Save gem names to checkpoint
            save_gem_names_checkpoint(gem_names)
            total_gems = len(gem_names)
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading gem names: {e}")
            return
    
    if total_gems == 0:
        print("No gems found.")
        return
    
    # Create the path to the output file
    output_dir = OUTPUT_DIR
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, OUTPUT_FILENAME)
    
    # Load already processed gems
    print("Checking for existing results...")
    processed_results = load_processed_gems(output_file)
    
    # Determine which gems still need processing
    total_to_process = total_gems - len(processed_results)
    
    if total_to_process <= 0:
        print("All gems already processed!")
        print(f"Results are in {output_file}")
        # Clean up checkpoint files
        print("Cleaning up checkpoint files...")
        if os.path.exists(GEM_NAMES_FILE):
            os.remove(GEM_NAMES_FILE)
        if os.path.exists(INDEX_CHECKPOINT):
            os.remove(INDEX_CHECKPOINT)
        if os.path.exists(CHECKPOINT_DIR):
            try:
                os.rmdir(CHECKPOINT_DIR)
            except OSError:
                pass
        print("Done!")
        return
    
    print(f"Total gems to process: {total_to_process:,}")
    print(f"Already processed: {len(processed_results):,} gems")
    print("Using parallel processing with 20 workers to avoid overwhelming the registry...")
    print("Processing in batches of 1000 gems to conserve memory...")
    print("Progress is saved after each batch for recovery")
    
    # Process gems in batches
    batch_size = 1000
    start_idx = 0
    processed_count = 0
    
    write_header = len(processed_results) == 0
    
    try:
        while start_idx < total_gems:
            # Load batch of gem names
            batch_gems = load_gem_names_batch(start_idx, batch_size)
            if not batch_gems:
                break
            
            # Create list of gems to process in this batch
            gems_to_process_batch = []
            for i, gem_name in enumerate(batch_gems):
                idx = start_idx + i + 1
                if idx not in processed_results:
                    gems_to_process_batch.append((idx, gem_name))
            
            if not gems_to_process_batch:
                start_idx += batch_size
                continue
            
            print(f"\nProcessing batch {start_idx // batch_size + 1}: gems {start_idx + 1} to {start_idx + len(batch_gems)}")
            print(f"  {len(gems_to_process_batch)} gems to process in this batch")
            
            new_results = {}
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                future_to_gem = {
                    executor.submit(fetch_gem_info, gem_name): (idx, gem_name)
                    for idx, gem_name in gems_to_process_batch
                }
                
                for future in tqdm(as_completed(future_to_gem), total=len(future_to_gem), desc=f"Batch {start_idx // batch_size + 1}"):
                    idx, original_name = future_to_gem[future]
                    try:
                        gem_name, homepage_url, repo_url = future.result()
                        new_results[idx] = (gem_name, homepage_url, repo_url)
                    except Exception as e:
                        new_results[idx] = (original_name, "nan", "nan")
                    
                    processed_count += 1
            
            # Save batch results to CSV immediately
            if new_results:
                print(f"  Saving {len(new_results)} gems to CSV...")
                save_results_incrementally(output_file, new_results, write_header)
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    print(f"  ✓ Saved to {output_file} (size: {file_size:,} bytes)")
                else:
                    print(f"  ✗ Warning: Output file not found after save!")
                write_header = False
            
            start_idx += batch_size
        
        print("\nAll processing complete!")
        print(f"Total gems processed: {processed_count:,}")
        print(f"Successfully saved to {output_file}")
        
        # Clean up checkpoint files
        print("Cleaning up checkpoint files...")
        if os.path.exists(GEM_NAMES_FILE):
            os.remove(GEM_NAMES_FILE)
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
    mine_ruby_gems()
