import re
import requests
import zipfile
import io
import argparse
from pyprojroot.here import here

def write_zip(output_dir):
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(output_dir)  # extract into a folder
        print("Extracted files:", z.namelist())

def main(url, location):
    
    OUTPUT_DIR = here("extract/input")
    location_norm = re.sub(" |-", "_", location.lower())

    r = requests.get(
        url=url, 
        headers= {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) ")
        })

    r.raise_for_status()  # ensure it downloaded successfully

    write_zip(OUTPUT_DIR / location_norm)    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('url', default="https://www.ssa.gov/oact/babynames/names.zip")
    parser.add_argument('location', default="United States")
    args = parser.parse_args()
    main(args.url, args.location)