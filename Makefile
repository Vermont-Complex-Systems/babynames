.PHONY: all scrape transform

URL ?= https://www.ssa.gov/oact/babynames/names.zip
COUNTRY ?= United States

scrape:
	uv run python extract/src/scrape.py $(URL) $(COUNTRY)

import:
	uv run python extract/src/import.py $(COUNTRY)

# prepare:
# 	uv run python adapter/src/prepare.py

# submit:
# 	uv run python adapter/src/submit.py 
