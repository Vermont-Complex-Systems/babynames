import requests
import zipfile
import io
import csv
from pyprojroot.here import here

def write_zip(output_dir, content):
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        z.extractall(output_dir)  # extract into a folder
        print("Extracted files:", z.namelist())

def write_csv(output_dir, content, filename):
    output_path = output_dir / filename
    output_path.write_bytes(content)
    print(f"Saved CSV to: {output_path}")


def scrape_from_csv(csv_path):
    """Read URLs from CSV and download all files"""
    OUTPUT_DIR = here("extract/input")

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row['url']
            filename = row['filename']
            location = row['location']

            # Create location-specific output directory
            output_path = OUTPUT_DIR / location
            output_path.mkdir(parents=True, exist_ok=True)

            # Check if data already exists
            if filename:
                # Check for specific CSV file
                target_file = output_path / filename
                if target_file.exists():
                    print(f"⏭ Skipping {location}/{filename} (already exists)")
                    continue
            else:
                # Check if directory has any files (for ZIP extracts)
                if any(output_path.iterdir()):
                    print(f"⏭ Skipping {location} (directory not empty)")
                    continue

            print(f"Downloading {location}: {url}")

            r = requests.get(
                url=url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            )

            r.raise_for_status()

            # Determine file type and save appropriately
            if filename:
                # CSV file with explicit filename
                write_csv(output_path, r.content, filename)
            elif url.endswith('.zip'):
                # ZIP file - extract to location directory
                write_zip(output_path, r.content)

def main():
    """Main entry point - scrape all URLs from CSV file"""
    import argparse

    parser = argparse.ArgumentParser(description="Download baby names data from URLs listed in CSV")
    parser.add_argument(
        'csv_path',
        nargs='?',
        default='extract/hand/list_urls.csv',
        help="Path to CSV file containing URLs (default: extract/hand/list_urls.csv)"
    )
    args = parser.parse_args()

    # Resolve path relative to project root
    csv_path = here() / args.csv_path

    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        return

    scrape_from_csv(csv_path)

if __name__ == '__main__':
    main()