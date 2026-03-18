# GSTR-1 JSON to Excel Converter

A production-ready Python script that processes 100+ GSTR-1 JSON files, flattening deeply nested substructures (like B2B, HSN, CDNR, etc.) and consolidating the data into a single multi-sheet Microsoft Excel (`.xlsx`) file. 

## Features
- Intelligently extracts all tabular fields completely normalized.
- Specifically unpacks complex nested hierarchies like `B2B -> INVOICES -> ITEMS`.
- Adds source metadata columns (`Source_File`, `GSTIN`, `Filing_Period`) inside all data chunks for tracking.
- Ignores corrupted JSON safely without crashing.
- Includes a Summary sheet containing aggregated tallies.

## Installation

1. Make sure Python 3.9+ is installed.
2. Install the requirements:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Open `gstr1_converter.py` and modify the `folder_path` variable to point to your JSON files.
2. Run the script:
   ```bash
   python gstr1_converter.py
   ```
3. A `GSTR1_Consolidated.xlsx` file will be generated in the same directory.
