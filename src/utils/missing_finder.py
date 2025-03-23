"""
Missing Spot Finder Module

This module provides functionality to identify surf spots that are missing
from the collected data, helping ensure completeness of the dataset.
"""

import pandas as pd
import os
from typing import Set, Dict, List, Any


def find_missing_locations(combined_file: str, locations_file: str) -> Set[str]:
    """
    Identifies locations that have time_of_year data but are missing from the surf analysis.

    Args:
        combined_file (str): Path to the combined surf analysis CSV
        locations_file (str): Path to the locations CSV with reference data

    Returns:
        set: Set of location names missing from surf analysis
    """
    # Read CSVs
    try:
        surf_df = pd.read_csv(combined_file)
        locations_df = pd.read_csv(locations_file)

        # Get unique spots from surf analysis
        surf_spots = set(surf_df['name'].unique())

        # Get locations with time_of_year data
        locations_with_time = locations_df[locations_df['time_of_year'].notna()]
        locations_set = set(locations_with_time['name'])

        # Find missing locations
        missing_locations = locations_set - surf_spots

        # Print analysis
        print(f"\nAnalysis Results:")
        print(f"Total locations with time_of_year data: {len(locations_set)}")
        print(f"Total spots in surf analysis: {len(surf_spots)}")
        print(f"Locations missing from surf analysis: {len(missing_locations)}")

        if missing_locations:
            print("\nMissing locations:")
            # Get additional info for missing locations
            missing_df = locations_with_time[locations_with_time['name'].isin(missing_locations)]
            missing_df = missing_df[['id', 'name', 'new_region']]
            missing_df = missing_df.sort_values(['new_region', 'name'])

            # Print missing locations grouped by region
            for region in missing_df['new_region'].unique():
                print(f"\nRegion: {region}")
                region_spots = missing_df[missing_df['new_region'] == region]
                for _, row in region_spots.iterrows():
                    print(f"- {row['name']} (ID: {row['id']})")

            # Save missing locations to CSV
            output_file = "missing_locations.csv"
            missing_df.to_csv(output_file, index=False)
            print(f"\nSaved missing locations to {output_file}")

        return missing_locations

    except Exception as e:
        print(f"Error processing files: {str(e)}")
        return set()


def verify_and_add_ids(processed_file: str, reference_file: str, output_file: str) -> pd.DataFrame:
    """
    Verifies data completeness and adds spot IDs from reference data.
    
    Args:
        processed_file (str): Path to the processed surf data CSV
        reference_file (str): Path to the reference data CSV with IDs
        output_file (str): Path to save the updated data CSV
        
    Returns:
        DataFrame: The updated dataframe with IDs added
    """
    # Load the data files
    print(f"Loading processed data from {processed_file}")
    processed_df = pd.read_csv(processed_file)

    print(f"Loading reference data from {reference_file}")
    reference_df = pd.read_csv(reference_file)

    # Filter reference data to only include rows with best_month values
    spots_with_best_month = reference_df[reference_df['best_month'].notna()]
    print(f"Found {len(spots_with_best_month)} spots with best_month values in reference file")

    # Months to check for
    all_months = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]

    # Create a dictionary to map spot names to their IDs
    spot_id_map = dict(zip(reference_df['name'], reference_df['id']))

    # Add ID column to processed data based on name
    processed_df['id'] = processed_df['name'].map(spot_id_map)

    # Create a list to store missing month information
    missing_months_info = []

    # Check each spot with a best_month value
    for _, row in spots_with_best_month.iterrows():
        spot_name = row['name']
        spot_id = row['id']
        spot_region = row['new_region']

        # Get all months for this spot in the processed data
        spot_data = processed_df[(processed_df['name'] == spot_name) &
                                (processed_df['new_region'] == spot_region)]

        if len(spot_data) == 0:
            # Spot not found in processed data at all
            missing_months_info.append({
                'id': spot_id,
                'name': spot_name,
                'region': spot_region,
                'missing_months': 'ALL - spot not found',
                'months_found': 0
            })
            continue

        # Check which months are present
        months_present = spot_data['month'].unique()
        missing_months = [month for month in all_months if month not in months_present]

        if missing_months:
            missing_months_info.append({
                'id': spot_id,
                'name': spot_name,
                'region': spot_region,
                'missing_months': ', '.join(missing_months),
                'months_found': len(months_present)
            })

    # Save the processed data with IDs
    processed_df.to_csv(output_file, index=False)
    print(f"Saved processed data with IDs to {output_file}")

    # Generate a report of missing months
    if missing_months_info:
        missing_df = pd.DataFrame(missing_months_info)
        missing_file = os.path.splitext(output_file)[0] + "_missing_months.csv"
        missing_df.to_csv(missing_file, index=False)
        print(f"WARNING: Found {len(missing_months_info)} spots with missing months")
        print(f"Missing months report saved to {missing_file}")

        # Print summary
        print("\nSummary of spots with missing months:")
        print(f"Total spots checked: {len(spots_with_best_month)}")
        print(f"Spots with missing months: {len(missing_months_info)}")

        # Spots missing all months
        completely_missing = sum(1 for item in missing_months_info if item['months_found'] == 0)
        print(f"Spots completely missing: {completely_missing}")

        # Spots with partial months
        partial_months = sum(1 for item in missing_months_info if 0 < item['months_found'] < 12)
        print(f"Spots with some months (but not all 12): {partial_months}")
    else:
        print("SUCCESS: All spots with best_month values have data for all 12 months")

    # Return the processed dataframe
    return processed_df


def check_missing_spots(locations_file: str, surf_data_file: str, output_folder: str) -> pd.DataFrame:
    """
    Check for spots with best_month values that are missing from surf data.
    
    Args:
        locations_file (str): Path to the locations reference CSV
        surf_data_file (str): Path to the surf data CSV
        output_folder (str): Folder to save output reports
        
    Returns:
        DataFrame: DataFrame containing missing spots information
    """
    # Load the locations data
    print(f"Loading locations data from {locations_file}")
    locations_df = pd.read_csv(locations_file)

    # Load the surf data
    print(f"Loading surf data from {surf_data_file}")
    surf_df = pd.read_csv(surf_data_file)

    # Clean column names to ensure consistency
    locations_df.columns = [col.strip() for col in locations_df.columns]
    surf_df.columns = [col.strip() for col in surf_df.columns]

    # Print basic stats
    print(f"\nLocations data: {len(locations_df)} rows")
    print(f"Surf data: {len(surf_df)} rows")

    # Filter locations to only those with best_month values
    if 'best_month' not in locations_df.columns:
        print("ERROR: 'best_month' column not found in locations data")
        return pd.DataFrame()

    locations_with_best_month = locations_df[locations_df['best_month'].notna()]
    print(f"\nFound {len(locations_with_best_month)} locations with best_month values")

    # Get unique spot IDs in the surf data
    surf_spot_ids = surf_df['id'].unique()
    print(f"Found {len(surf_spot_ids)} unique spot IDs in surf data")

    # Find spots that are in locations (with best_month) but missing from surf data
    missing_spots = locations_with_best_month[~locations_with_best_month['id'].isin(surf_spot_ids)]

    print(f"\nRESULTS:")
    print(f"- {len(missing_spots)} out of {len(locations_with_best_month)} spots with best_month values are missing from surf data")
    print(f"- {len(locations_with_best_month) - len(missing_spots)} spots with best_month values are present in surf data")

    if len(missing_spots) > 0:
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Save missing spots to CSV
        missing_file = f"{output_folder}/missing_best_month_spots.csv"
        missing_spots.to_csv(missing_file, index=False)
        print(f"\nSaved list of missing spots to {missing_file}")

        # Show some examples
        print("\nExamples of missing spots:")
        sample = min(5, len(missing_spots))
        for i, (_, row) in enumerate(missing_spots.head(sample).iterrows()):
            print(f"{i+1}. ID: {row['id']} - {row['name']} ({row['new_region']}) - Best month: {row['best_month']}")

        if len(missing_spots) > sample:
            print(f"... and {len(missing_spots) - sample} more")

        # Generate additional statistics about missing spots
        if 'continent' in missing_spots.columns:
            continent_counts = missing_spots['continent'].value_counts()
            print("\nMissing spots by continent:")
            for continent, count in continent_counts.items():
                print(f"- {continent}: {count} spots")

        if 'rating' in missing_spots.columns:
            high_rated = missing_spots[missing_spots['rating'] >= 7]
            if len(high_rated) > 0:
                print(f"\n{len(high_rated)} missing spots have high ratings (≥7)")
                high_rated_file = f"{output_folder}/missing_high_rated_spots.csv"
                high_rated.to_csv(high_rated_file, index=False)
                print(f"Saved to {high_rated_file}")

    return missing_spots
