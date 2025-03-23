"""
Surf Spot Analyzer Module

This module provides functionality to analyze multiple surf spots 
by extracting data for each month and compiling results into a dataset.
"""

import pandas as pd
import time
from tqdm import tqdm
import os
from typing import List, Dict, Any, Optional

from src.extractors.gif_extractor import extract_surf_data_from_url


def analyze_surf_spots(csv_path: str, base_url: str, output_path: str = "surf_analysis.csv", 
                       limit: int = 5500, start: int = 0) -> List[Dict[str, Any]]:
    """
    Analyze surf spots from CSV file that have time_of_year data.
    
    This function:
    1. Reads a CSV file containing surf spot information
    2. Filters spots that have time_of_year data
    3. For each spot, extracts surf condition data for all months
    4. Compiles the data into a comprehensive dataset
    
    Args:
        csv_path (str): Path to the CSV file with spot information
        base_url (str): Base URL for the GIF images
        output_path (str): Path to save the output CSV file
        limit (int): Maximum number of spots to process
        start (int): Starting index for spot processing
        
    Returns:
        list: List of dictionaries containing analyzed surf data
    """
    # Read locations CSV
    df = pd.read_csv(csv_path)
    results = []

    # Filter spots with time_of_year data and apply limits
    spots = df[df['time_of_year'].notna()].iloc[start:limit]
    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']

    for _, spot in tqdm(spots.iterrows(), total=min(len(spots), limit-start), desc="Processing spots"):
        print(f"\nProcessing {spot['name']}")
        name_for_url = spot['name'].replace(' ', '-')

        for month in months:
            url = f"{base_url}/{name_for_url}.surf.consistency.{month}.gif"
            try:
                data = extract_surf_data_from_url(url, spot['name'], month)
                if data:
                    # Add additional columns from locations.csv
                    data['name'] = spot['name']
                    data['new_region'] = spot['new_region']

                    results.append(data)

                    print(f"Processed {spot['name']} - {month}")
                    print(f"Wave heights: flat={data['flat']}%, 0.5-1.3m={data['height_0_4']}%, "
                          f"1.3-2m={data['height_4_6']}%, 2-3m={data['height_6_10']}%, >3m={data['height_10_plus']}%")

                    # Save intermediate results periodically
                    if len(results) % 10 == 0:  # Save every 10 spots
                        intermediate_df = pd.DataFrame(results)
                        # Reorder columns
                        intermediate_df = intermediate_df[[
                            'name', 'new_region', 'month',
                            'clean', 'blown_out', 'too_small',
                            'flat', 'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus'
                        ]]
                        intermediate_df.to_csv(f"{output_path}_intermediate.csv", index=False)
                        print(f"Saved intermediate results, processed {len(results)} entries")

                time.sleep(0.5)  # Add delay between requests to avoid overloading the server

            except Exception as e:
                print(f"Error processing {spot['name']} - {month}: {str(e)}")
                continue

    # Save final results
    if results:
        final_df = pd.DataFrame(results)
        final_df = final_df[[
            'name', 'new_region', 'month',
            'clean', 'blown_out', 'too_small',
            'flat', 'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus'
        ]]
        final_df.to_csv(output_path, index=False)
        print(f"\nFinal results saved to {output_path}")

    return results


def analyze_missing_spots(csv_path: str, new_merge_path: str, output_path: str = "missing_spots_analysis.csv", 
                         start_index: int = 0, end_index: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Analyze surf spots from CSV file that have direct GIF URLs, 
    focusing on spots that were missing from previous analysis.
    
    Args:
        csv_path (str): Path to the CSV file with missing spot information
        new_merge_path (str): Path to the new merge file to check for existing entries
        output_path (str): Path to save the output CSV file
        start_index (int): Starting index for spot processing
        end_index (int, optional): Ending index for spot processing
        
    Returns:
        list: List of dictionaries containing analyzed surf data for missing spots
    """
    # Read locations CSV and filter out rows with NA in gif_url
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=['gif_url'])

    # Read new merge file if it exists
    new_merge_df = pd.DataFrame()
    if os.path.exists(new_merge_path):
        new_merge_df = pd.read_csv(new_merge_path)
        print(f"Loaded new merge file with {len(new_merge_df)} entries")

    # Read existing results if available
    existing_results = pd.DataFrame()
    if os.path.exists(output_path):
        existing_results = pd.read_csv(output_path)

    results = []
    if not existing_results.empty:
        results = existing_results.to_dict('records')

    # Handle index range
    if end_index is None:
        end_index = len(df)

    # Validate indices
    start_index = max(0, min(start_index, len(df)))
    end_index = max(start_index, min(end_index, len(df)))

    # Select spots within the specified range
    spots = df.iloc[start_index:end_index]

    print(f"Processing spots from index {start_index} to {end_index-1}")

    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']

    for idx, spot in tqdm(spots.iterrows(), total=len(spots), desc="Processing spots"):
        # Check if spot exists in new merge file
        if not new_merge_df.empty and len(new_merge_df[new_merge_df['name'].str.lower() == spot['name'].lower()]) > 0:
            continue

        # Get the base URL from the gif_url by removing the month and .gif
        base_gif_url = spot['gif_url'].rsplit('.', 2)[0]

        for month in months:
            # Skip if this spot-month combination already exists in results
            if existing_results.empty is False and len(existing_results[
                (existing_results['name'] == spot['name']) &
                (existing_results['month'].str.lower() == month)
            ]) > 0:
                continue

            # Construct the full URL for each month
            url = f"{base_gif_url}.{month}.gif"
            try:
                data = extract_surf_data_from_url(url, spot['name'], month)
                if data:
                    # Add additional columns from the CSV
                    data['name'] = spot['name']
                    data['new_region'] = spot['new_region']

                    results.append(data)

                    # Save intermediate results periodically
                    if len(results) % 10 == 0:  # Save every 10 spots
                        intermediate_df = pd.DataFrame(results)
                        intermediate_df = intermediate_df[[
                            'name', 'new_region', 'month',
                            'clean', 'blown_out', 'too_small',
                            'flat', 'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus'
                        ]]
                        intermediate_df.to_csv(f"{output_path}_intermediate.csv", index=False)
                        print(f"Saved intermediate results, processed {len(results)} entries")

                time.sleep(0.5)  # Add delay between requests

            except Exception as e:
                print(f"Error processing {spot['name']} - {month}: {str(e)}")
                continue

    # Save final results
    if results:
        final_df = pd.DataFrame(results)
        final_df = final_df[[
            'name', 'new_region', 'month',
            'clean', 'blown_out', 'too_small',
            'flat', 'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus'
        ]]
        final_df.to_csv(output_path, index=False)
        print(f"\nFinal results saved to {output_path}")

    return results
