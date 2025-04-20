import csv
import sys
import re
from tqdm import tqdm
import concurrent.futures
import os
import math

# Increase the field size limit
csv.field_size_limit(sys.maxsize)

def load_countries():
    """Load country names and variations from the CSV file."""
    countries = {}
    # Common words to exclude from matching to avoid false positives
    exclude_terms = {'of', 'the', 'in', 'on', 'at', 'by', 'with', 'to', 'from', 'for', 'new', 
                    'east', 'west', 'north', 'south', 'central'}
    
    print("Loading country data...")
    with open('data/raw/countries.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if len(row) >= 2:
                country_name = row[0].strip()
                variations = [v.strip() for v in row[1].split() if v.strip()]
                # Add the main country name to variations
                variations.append(country_name)
                
                # Filter out common words
                filtered_variations = []
                for var in variations:
                    if var.lower() not in exclude_terms and len(var) > 2:  # Minimum length to avoid matching 2-letter words
                        filtered_variations.append(var)
                
                if filtered_variations:
                    countries[country_name] = filtered_variations
    print(f"Loaded {len(countries)} countries with variations.")
    return countries

def compile_country_patterns(countries_dict):
    """Compile regex patterns for each country."""
    country_patterns = {}
    print("Compiling regex patterns...")
    for country, variations in tqdm(countries_dict.items(), desc="Compiling country patterns"):
        # Sort variations by length (descending) to match longer phrases first
        sorted_variations = sorted(variations, key=len, reverse=True)
        pattern_parts = [re.escape(var) for var in sorted_variations]
        # Create a pattern that matches word boundaries
        pattern = r'\b(' + '|'.join(pattern_parts) + r')\b'
        country_patterns[country] = re.compile(pattern, re.IGNORECASE)
    return country_patterns

def find_countries_in_text(text, country_patterns):
    """Find country references in a text."""
    found_countries = []
    
    for country, pattern in country_patterns.items():
        matches = pattern.findall(text)
        if matches:
            found_countries.append(country)
    
    return found_countries

def process_chunk(chunk_data, country_patterns, start_row):
    """Process a chunk of rows."""
    results = []
    for i, row in enumerate(chunk_data):
        row_num = start_row + i
        if len(row) >= 4:
            title = row[0]
            abstract = row[1]
            journal = row[2]
            date = row[3]
            
            # Find countries in title and abstract
            title_countries = find_countries_in_text(title, country_patterns)
            abstract_countries = find_countries_in_text(abstract, country_patterns)
            
            # Only include rows with at least one country in title or abstract
            if title_countries or abstract_countries:
                results.append({
                    'row_num': row_num,
                    'title': title,
                    'abstract': abstract,
                    'journal': journal,
                    'date': date,
                    'title_country': ', '.join(title_countries),
                    'abstract_country': ', '.join(abstract_countries)
                })
    return results

def create_country_csv():
    """Create a CSV with country info from abstract dataset."""
    countries = load_countries()
    country_patterns = compile_country_patterns(countries)
    
    # First count the total number of rows for the progress bar
    row_count = 0
    print("Counting rows in CSV file...")
    with open('data/raw/abstract_1per_sample.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for _ in reader:
            row_count += 1
    print(f"Found {row_count} rows to process.")
    
    # Determine chunk size and number of workers
    # Get CPU count, but leave one core free for system processes
    num_workers = max(1, os.cpu_count() - 1)
    chunk_size = min(10000, math.ceil(row_count / num_workers))
    num_chunks = math.ceil(row_count / chunk_size)
    
    print(f"Processing with {num_workers} workers in {num_chunks} chunks of ~{chunk_size} rows each")
    
    # Process the file in chunks
    all_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_chunk = {}
        
        # Read all data and prepare chunks for processing
        print("Reading data and submitting tasks...")
        chunks = []
        with open('data/raw/abstract_1per_sample.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            
            current_chunk = []
            for i, row in enumerate(reader, 1):
                current_chunk.append(row)
                if len(current_chunk) >= chunk_size:
                    chunks.append(current_chunk)
                    current_chunk = []
            # Add the last chunk if not empty
            if current_chunk:
                chunks.append(current_chunk)
        
        # Submit tasks for parallel processing
        for i, chunk in enumerate(chunks):
            start_row = i * chunk_size + 1
            future = executor.submit(process_chunk, chunk, country_patterns, start_row)
            future_to_chunk[future] = i
            
        # Process results as they complete
        with tqdm(total=len(future_to_chunk), desc="Processing chunks") as pbar:
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_results = future.result()
                    all_results.extend(chunk_results)
                    pbar.update(1)
                    print(f"Chunk {chunk_idx+1}/{len(chunks)} complete. Found {len(chunk_results)} mentions. Total: {len(all_results)}")
                except Exception as e:
                    print(f"Chunk {chunk_idx} generated an exception: {e}")
    
    # Write results to CSV
    print("Writing results to country_mentions.csv...")
    with open('data/processed/country_mentions.csv', 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['row_num', 'title', 'abstract', 'journal', 'date', 'title_country', 'abstract_country']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)
    
    print(f"Created CSV with {len(all_results)} entries containing country mentions.")

if __name__ == "__main__":
    create_country_csv() 