import csv
import pandas as pd

def sort_csv_by_row_num():
    """Sort the country_mentions.csv file by row_num in ascending order."""
    print("Reading country_mentions.csv file...")
    
    # Read the CSV file using pandas for easier sorting
    df = pd.read_csv('data/processed/country_mentions.csv')
    
    # Convert row_num to integer for proper sorting
    df['row_num'] = df['row_num'].astype(int)
    
    # Sort by row_num
    print("Sorting by row_num...")
    sorted_df = df.sort_values(by='row_num')
    
    # Write sorted data back to CSV
    print("Writing sorted data to country_mentions_sorted.csv...")
    sorted_df.to_csv('data/processed/country_mentions_sorted.csv', index=False)
    
    print("Done! Sorted data saved to data/processed/country_mentions_sorted.csv")
    print(f"Total entries: {len(sorted_df)}")

if __name__ == "__main__":
    sort_csv_by_row_num() 