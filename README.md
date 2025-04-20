# Country Mentions Analysis

This project analyzes a dataset of research article abstracts to identify mentions of countries in both titles and abstracts.

## Project Structure

```
.
├── data
│   ├── processed      # Processed data files
│   └── raw            # Original data files
├── scripts            # Python scripts for data processing
└── requirements.txt   # Required Python packages
```

## Data Files

### Raw Data
- `countries.csv` - List of countries and their variations for matching
- `abstract_1per_sample.csv` - Original dataset with article abstracts (not included in repository)

### Processed Data
- `country_mentions.csv` - Extracted articles that mention countries
- `country_mentions_sorted.csv` - Same data sorted by row number

## Important Note About Raw Data

**The main dataset file `abstract_1per_sample.csv` is not included in this repository due to its size.**

To use this project:
1. Place your CSV file containing article data in the `data/raw/` directory
2. Ensure it is named `abstract_1per_sample.csv`
3. The CSV file should have the following columns in this order:
   - title
   - abstract
   - journal
   - date

## Scripts

- `create_country_mentions.py` - Main script that processes the dataset and extracts articles with country mentions
- `sort_country_mentions.py` - Script to sort the country mentions data by row number

## Setup

1. Install required packages:
   ```
   pip install -r requirements.txt
   ```

2. Place the required raw data file in the correct location:
   ```
   data/raw/abstract_1per_sample.csv
   ```

3. Run the main script to extract country mentions:
   ```
   python scripts/create_country_mentions.py
   ```

4. Sort the results:
   ```
   python scripts/sort_country_mentions.py
   ```


## Results

The analysis found 165,287 research articles (out of 851,498) that mention countries in their titles or abstracts. 