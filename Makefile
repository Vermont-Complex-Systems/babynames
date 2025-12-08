.PHONY: all scrape import prepare submit

# Paths
HAND_DIR := extract/hand
URL_LIST := $(HAND_DIR)/list_urls.csv

# Variables
COUNTRY ?= 'United States'

scrape:
	uv run python extract/src/scrape.py $(URL_LIST)

import:
	uv run python extract/src/import.py $(COUNTRY)

prepare:
	uv run python adapter/src/prepare.py

submit:
	uv run python adapter/src/submit.py 
