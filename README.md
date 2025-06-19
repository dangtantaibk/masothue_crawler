# MST Crawler - Mã Số Thuế Company Data Crawler

A Python-based web crawler for extracting Vietnamese company information by tax codes (Mã Số Thuế). The crawler supports batch processing for handling large datasets efficiently.

## Project Structure

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── company_slugs.csv          # Main input file with company slugs
├── debug_response.html
├── batches/                   # Split CSV files for batch processing
│   ├── company_slugs_batch_001.csv
│   ├── company_slugs_batch_002.csv
│   └── ...
├── config/                    # Configuration files
├── crawled_results/           # Output directory for crawled data
└── src/
    └── mst_crawler.py         # Main crawler implementation
```

## Features

- **Batch Processing**: Automatically splits large CSV files into smaller batches for efficient processing
- **Company Data Extraction**: Crawls detailed company information using tax codes
- **Excel Integration**: Reads Excel files and generates company slugs
- **CSV Management**: Save and merge results in CSV format
- **Error Handling**: Robust error handling with logging
- **Configurable Batch Size**: Customize batch size based on system capabilities

## Installation

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Process company slugs from CSV file with batch processing:

```python
from src.mst_crawler import process_company_slugs_in_batches

# Process with default batch size (100)
process_company_slugs_in_batches("company_slugs.csv")

# Process with custom batch size
process_company_slugs_in_batches("company_slugs.csv", batch_size=50)
```

### Generate Company Slugs from Excel

```python
from src.mst_crawler import read_excel_and_generate_slugs, save_to_csv_pandas

# Read Excel file and generate slugs
file_path = "your_excel_file.xlsx"
results = read_excel_and_generate_slugs(file_path)

# Save to CSV
save_to_csv_pandas(results, "company_slugs.csv")
```

### Merge Batch Results

After batch processing, merge all results into a single file:

```python
from src.mst_crawler import merge_batch_results

# Merge all batch results
merge_batch_results(results_dir="crawled_results", output_file="final_crawled_data.csv")
```

## Key Functions

### [`split_csv_into_batches`](src/mst_crawler.py)
Splits a large CSV file into smaller batch files for processing.

### [`process_company_slugs_in_batches`](src/mst_crawler.py)
Main function that processes company slugs in batches, crawls data, and saves results.

### [`crawl_batch_data`](src/mst_crawler.py)
Crawls company data for a specific batch of company slugs.

### [`merge_batch_results`](src/mst_crawler.py)
Combines all batch result files into a single output file.

### [`save_to_csv_pandas`](src/mst_crawler.py)
Saves data to CSV format using pandas.

## Workflow

1. **Input Preparation**: Place your company data in [`company_slugs.csv`](company_slugs.csv) or generate from Excel
2. **Batch Processing**: The system automatically splits the input into batches in the [`batches/`](batches/) directory
3. **Data Crawling**: Each batch is processed and results are saved to [`crawled_results/`](crawled_results/)
4. **Result Merging**: All batch results are combined into a final output file

## Configuration

- **Batch Size**: Default is 100, can be customized based on system performance
- **Output Directory**: Results are saved to `crawled_results/` by default
- **Headers**: Crawler uses predefined headers for web requests

## Error Handling

The crawler includes comprehensive error handling:
- Failed batches are logged and skipped
- Individual company crawling errors don't stop the entire process
- Detailed logging for debugging

## Example

```python
# Complete workflow example
from src.mst_crawler import process_company_slugs_in_batches, merge_batch_results

# Step 1: Process company slugs in batches
process_company_slugs_in_batches("company_slugs.csv", batch_size=100)

# Step 2: Merge all results
merge_batch_results("crawled_results", "final_company_data.csv")
```

## Requirements

See [`requirements.txt`](requirements.txt) for full dependency list. Main dependencies include:
- pandas
- requests
- logging

## Notes

- The crawler is designed specifically for Vietnamese company tax code data
- Batch processing helps manage memory usage and network requests
- All CSV files use UTF-8 encoding for Vietnamese character support

## License

[Add your license information here]