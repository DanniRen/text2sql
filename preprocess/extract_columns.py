#!/usr/bin/env python3
import json
import csv
import os

def extract_column_names_to_csv(json_file_path, output_csv_path):
    """
    Extract column_names_original from JSON file and convert to CSV with index identifiers
    """
    try:
        # Load JSON data
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        # Prepare CSV data
        csv_data = []
        
        # Iterate through each table entry in the JSON
        for idx, db in enumerate(data):
            if 'column_names_original' in db:
                column_names = db['column_names_original']
                
                # Extract column names (second element of each pair)
                for col_idx, column_pair in enumerate(column_names):
                    if len(column_pair) >= 2:
                        column_name = column_pair[1]
                        table_idx = column_pair[0]
                        
                        # Add row with index identifier and column name
                        csv_data.append({
                            'column_index': col_idx,
                            'table_index': table_idx,
                            'column_name': column_name
                        })
        
        # Write to CSV file
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            fieldnames = ['table_index', 'column_index', 'column_name']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for row in csv_data:
                writer.writerow(row)
        
        print(f"Successfully extracted {len(csv_data)} column names to {output_csv_path}")
        return True
        
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
        return False
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file_path}")
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    # Define input and output paths
    input_json = "tables.json"
    output_csv = "column_names_original.csv"
    
    # Check if input file exists
    if not os.path.exists(input_json):
        print(f"Error: Input file {input_json} does not exist")
        exit(1)
    
    # Extract and convert
    success = extract_column_names_to_csv(input_json, output_csv)
    
    if success:
        print("Extraction completed successfully!")
    else:
        print("Extraction failed!")