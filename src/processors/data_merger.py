"""
Data Merger Module

This module provides functionality to merge multiple CSV files containing
surf data, handle duplicates, and ensure data integrity.
"""

import pandas as pd
import os
from glob import glob
from typing import Dict, Any, Optional


def merge_csvs(folder_path: str, output_file: str) -> Optional[pd.DataFrame]:
    """
    Merge all CSV files in a folder into a single CSV file.
    Handles duplicates by keeping the last occurrence.

    Args:
        folder_path (str): Path to folder containing CSV files
        output_file (str): Path for output CSV file
        
    Returns:
        DataFrame or None: The merged dataframe if successful, None otherwise
    """
    # Get all CSV files in the folder
    csv_files = glob(os.path.join(folder_path, "*.csv"))
    print(f"Found {len(csv_files)} CSV files")

    # Read and combine all CSVs
    all_dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            print(f"Loaded {len(df)} rows from {os.path.basename(file)}")
            all_dfs.append(df)
        except Exception as e:
            print(f"Error loading {file}: {str(e)}")

    if not all_dfs:
        print("No CSV files were loaded successfully!")
        return None

    # Combine all dataframes
    print("\nMerging files...")
    merged_df = pd.concat(all_dfs, ignore_index=True)

    # Remove duplicates based on name and month
    initial_rows = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['name', 'month'], keep='last')
    duplicates_removed = initial_rows - len(merged_df)

    print(f"\nStats:")
    print(f"Total rows before deduplication: {initial_rows}")
    print(f"Duplicates removed: {duplicates_removed}")
    print(f"Final row count: {len(merged_df)}")

    # Save merged results
    merged_df.to_csv(output_file, index=False)
    print(f"\nSaved merged results to {output_file}")
    
    return merged_df


def correct_and_merge_data(existing_file: str, new_file: str, reference_file: str, 
                          output_file: str) -> Dict[str, Any]:
    """
    Correct IDs in existing data and merge with new data.
    
    This function:
    1. Loads existing data and new data
    2. Corrects IDs in existing data based on reference data
    3. Merges datasets, handling duplicates
    4. Checks completeness of the merged data
    
    Args:
        existing_file (str): Path to the existing data CSV
        new_file (str): Path to the new data CSV
        reference_file (str): Path to the reference data CSV with correct IDs
        output_file (str): Path to save the merged output CSV
        
    Returns:
        dict: Statistics about the merge operation
    """
    # Load the data files
    print(f"Loading existing data from {existing_file}")
    try:
        existing_df = pd.read_csv(existing_file)
    except Exception as e:
        print(f"Error loading existing file: {e}")
        try:
            # Try with flexible parser
            existing_df = pd.read_csv(existing_file, sep=None, engine='python')
        except Exception as e2:
            print(f"Fatal error loading existing file: {e2}")
            return {"error": "Failed to load existing file"}

    # Load the new data
    print(f"Loading new data from {new_file}")
    try:
        new_df = pd.read_csv(new_file, delimiter='\t')
    except Exception as e:
        print(f"Error with tab delimiter: {e}")
        try:
            # Try comma delimiter
            new_df = pd.read_csv(new_file)
        except Exception as e2:
            print(f"Error with comma delimiter: {e2}")
            try:
                # Last resort - flexible parser
                new_df = pd.read_csv(new_file, sep=None, engine='python')
            except Exception as e3:
                print(f"Fatal error loading new file: {e3}")
                return {"error": "Failed to load new file"}

    # Load the reference data for IDs and completeness check
    print(f"Loading reference data from {reference_file}")
    try:
        reference_df = pd.read_csv(reference_file)
    except Exception as e:
        print(f"Error loading reference file: {e}")
        try:
            # Try with flexible parser
            reference_df = pd.read_csv(reference_file, sep=None, engine='python')
        except Exception as e2:
            print(f"Fatal error loading reference file: {e2}")
            return {"error": "Failed to load reference file"}

    # Clean column names
    existing_df.columns = [col.strip() for col in existing_df.columns]
    new_df.columns = [col.strip() for col in new_df.columns]
    reference_df.columns = [col.strip() for col in reference_df.columns]

    # Check required columns in reference data
    ref_required_cols = ['id', 'name', 'new_region']
    missing_ref_cols = [col for col in ref_required_cols if col not in reference_df.columns]
    if missing_ref_cols:
        print(f"Reference file missing crucial columns: {missing_ref_cols}")
        return {"error": "Reference file missing crucial columns"}

    # Create a spot identifier using both name and new_region for reference data
    print("\nCreating unique spot identifiers using name and region...")
    reference_df['spot_identifier'] = reference_df['name'] + '|' + reference_df['new_region']

    # Create a mapping from spot_identifier to ID
    id_mapping = dict(zip(reference_df['spot_identifier'], reference_df['id']))

    print(f"Created ID mapping for {len(id_mapping)} unique spots from reference data")

    # STEP 1: First correct IDs in the existing data
    print("\nCORRECTING IDs IN EXISTING DATA...")

    # Create spot_identifier for existing data
    existing_df['spot_identifier'] = existing_df['name'] + '|' + existing_df['new_region']

    # Store original ID for comparison
    if 'id' in existing_df.columns:
        existing_df['original_id'] = existing_df['id']

    # Apply correct IDs from the mapping
    existing_df['id'] = existing_df['spot_identifier'].map(id_mapping)

    # Count how many IDs were changed
    if 'original_id' in existing_df.columns:
        id_changes = (existing_df['id'] != existing_df['original_id']).sum()
        print(f"Corrected {id_changes} IDs in existing data")
        # Remove the temporary column
        existing_df = existing_df.drop(columns=['original_id'])
    else:
        print(f"Added IDs to {existing_df['id'].notna().sum()} rows in existing data")

    # STEP 2: Process new data
    print("\nPROCESSING NEW DATA...")

    # Define required columns for processing new data
    required_columns = [
        'name', 'new_region', 'month', 'clean',
        'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus'
    ]

    # Check if all required columns exist in new data
    missing_cols = [col for col in required_columns if col not in new_df.columns]
    if missing_cols:
        print(f"\nWARNING: New data is missing these required columns: {missing_cols}")
        print("Will only use rows from existing data")
        filtered_new_df = pd.DataFrame(columns=new_df.columns)  # Empty DataFrame
    else:
        # Filter the new data to only include rows with values in all required columns
        print("\nFiltering new data to only include complete rows...")
        filtered_new_df = new_df.dropna(subset=required_columns)
        print(f"Retained {len(filtered_new_df)} out of {len(new_df)} rows from new data")

        # Create spot_identifier for new data
        filtered_new_df['spot_identifier'] = filtered_new_df['name'] + '|' + filtered_new_df['new_region']

        # Assign correct IDs using the mapping
        filtered_new_df['id'] = filtered_new_df['spot_identifier'].map(id_mapping)

        # Filter out rows that couldn't be matched to an ID
        valid_id_count = filtered_new_df['id'].notna().sum()
        if valid_id_count < len(filtered_new_df):
            print(f"WARNING: Could not find IDs for {len(filtered_new_df) - valid_id_count} rows in new data")
            print("These rows will be excluded from the merge")

        filtered_new_df = filtered_new_df[filtered_new_df['id'].notna()]
        print(f"Found valid IDs for {len(filtered_new_df)} spots in new data")

    # If we have valid new data, calculate skill levels
    if len(filtered_new_df) > 0:
        print("Calculating skill levels for new data...")
        # Ensure data is numeric
        numeric_cols = ['height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus', 'clean']
        for col in numeric_cols:
            filtered_new_df[col] = pd.to_numeric(filtered_new_df[col], errors='coerce')

        # Beginners: primarily 0-4ft waves
        filtered_new_df['beginner'] = filtered_new_df['height_0_4']

        # Intermediates: primarily 4-6ft waves plus some smaller waves
        filtered_new_df['intermediate'] = filtered_new_df['height_4_6'] + (filtered_new_df['height_0_4'] * 0.5)

        # Advanced: 6ft+ waves plus most of the 4-6ft waves
        filtered_new_df['advanced'] = filtered_new_df['height_6_10'] + filtered_new_df['height_10_plus'] + (filtered_new_df['height_4_6'] * 0.8)

        # Factor in "clean" percentage
        filtered_new_df['beginner'] = filtered_new_df['beginner'] * (filtered_new_df['clean'] / 100)
        filtered_new_df['intermediate'] = filtered_new_df['intermediate'] * ((filtered_new_df['clean'] + 15) / 100)
        filtered_new_df['advanced'] = filtered_new_df['advanced'] * ((filtered_new_df['clean'] + 25) / 100)

        # Cap values at 100 and round
        filtered_new_df[['beginner', 'intermediate', 'advanced']] = filtered_new_df[['beginner', 'intermediate', 'advanced']].clip(0, 100)
        filtered_new_df[['beginner', 'intermediate', 'advanced']] = filtered_new_df[['beginner', 'intermediate', 'advanced']].round(1)

    # STEP 3: Merge datasets
    print("\nMERGING DATASETS...")

    # Drop url_pattern and spot_identifier before merging
    columns_to_drop = ['url_pattern', 'spot_identifier']
    for col in columns_to_drop:
        if col in filtered_new_df.columns:
            filtered_new_df = filtered_new_df.drop(columns=[col])
        if col in existing_df.columns:
            existing_df = existing_df.drop(columns=[col])

    # Define key columns for identifying duplicates
    key_columns = ['id', 'name', 'new_region', 'month']

    # Combine the dataframes
    print("Combining datasets and handling duplicates...")
    combined_df = pd.concat([existing_df, filtered_new_df], ignore_index=True)

    # Remove duplicates, keeping the first occurrence (from existing data)
    before_dedup = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=key_columns, keep='first')
    after_dedup = len(combined_df)
    print(f"Removed {before_dedup - after_dedup} duplicate rows")

    # Save the combined data
    combined_df.to_csv(output_file, index=False)
    print(f"Saved merged data to {output_file}")

    # STEP 4: Check completeness
    print("\nCHECKING DATA COMPLETENESS...")

    # Filter reference data to only include spots with best_month values
    if 'best_month' in reference_df.columns:
        spots_to_check = reference_df[(reference_df['id'].notna()) & (reference_df['best_month'].notna())]
        print(f"Found {len(spots_to_check)} spots with IDs and best_month values in reference file")
    else:
        print("Reference file doesn't have 'best_month' column, checking all spots with IDs")
        spots_to_check = reference_df[reference_df['id'].notna()]
        print(f"Found {len(spots_to_check)} spots with IDs in reference file")

    # All months that should be present
    all_months = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]

    # Track missing data
    missing_data = []
    total_missing_months = 0

    # Check each spot
    for _, row in spots_to_check.iterrows():
        spot_id = row['id']
        spot_name = row['name']
        spot_region = row['new_region']

        # Get months for this spot in the combined data
        spot_data = combined_df[(combined_df['id'] == spot_id) &
                               (combined_df['name'] == spot_name) &
                               (combined_df['new_region'] == spot_region)]

        if len(spot_data) == 0:
            # Spot completely missing
            missing_data.append({
                'id': spot_id,
                'name': spot_name,
                'region': spot_region,
                'missing_months': 'ALL',
                'months_found': 0,
                'total_missing': 12
            })
            total_missing_months += 12
            continue

        # Check which months are present
        months_present = spot_data['month'].unique()
        missing_months = [month for month in all_months if month not in months_present]

        if missing_months:
            missing_data.append({
                'id': spot_id,
                'name': spot_name,
                'region': spot_region,
                'missing_months': ', '.join(missing_months),
                'months_found': len(months_present),
                'total_missing': len(missing_months)
            })
            total_missing_months += len(missing_months)

    # Generate report on missing data
    if missing_data:
        missing_df = pd.DataFrame(missing_data)
        missing_file = os.path.splitext(output_file)[0] + "_missing_report.csv"
        missing_df.to_csv(missing_file, index=False)

        # Calculate total expected data points
        total_expected = len(spots_to_check) * 12

        print(f"\nMISSING DATA SUMMARY:")
        print(f"Total spots: {len(spots_to_check)}")
        print(f"Total expected monthly data points: {total_expected}")
        print(f"Total missing monthly data points: {total_missing_months}")
        print(f"Completion rate: {((total_expected - total_missing_months) / total_expected) * 100:.2f}%")
        print(f"Spots with missing data: {len(missing_data)}")
        print(f"Detailed missing data report saved to {missing_file}")
    else:
        print("SUCCESS: All spots have complete data for all 12 months")

    # Return statistics
    return {
        'total_spots': len(spots_to_check),
        'total_data_points': len(combined_df),
        'missing_months': total_missing_months,
        'spots_with_missing_data': len(missing_data) if missing_data else 0
    }
