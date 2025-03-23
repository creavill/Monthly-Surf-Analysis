"""
Temperature Collector Module

This module collects average temperature data for surf regions using NASA's POWER API
and integrates it with the surf data for more comprehensive analysis.
"""

import pandas as pd
import sqlite3
import numpy as np
import requests
from datetime import datetime
import time
from typing import Optional, Dict, Any, List


def get_avg_temp(lat: float, lon: float, month: str) -> Optional[float]:
    """
    Get average temperature for a specific latitude, longitude,
    from 1st to 20th of the month
    
    Args:
        lat (float): Latitude coordinate
        lon (float): Longitude coordinate
        month (str): Month name (e.g., 'January')
        
    Returns:
        float or None: Temperature in Celsius or None if data could not be retrieved
    """
    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"

    # Convert month name to date strings (2023-MM-01 to 2023-MM-20)
    month_num = datetime.strptime(month, '%B').month
    start_date = f"2023{month_num:02d}01"  # Format: YYYYMMDD
    end_date = f"2023{month_num:02d}20"    # Format: YYYYMMDD

    params = {
        'parameters': 'T2M',  # Temperature at 2 meters
        'community': 'RE',
        'longitude': round(lon, 4),
        'latitude': round(lat, 4),
        'format': 'JSON',
        'start': start_date,
        'end': end_date
    }

    try:
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if 'properties' in data and 'parameter' in data['properties']:
                temps = data['properties']['parameter']['T2M']
                # Calculate average of all temperatures in the date range
                avg_temp = sum(temps.values()) / len(temps)
                return round(avg_temp, 2)
            else:
                print(f"No temperature data found for {month} at {lat}, {lon}")
                return None
        else:
            print(f"Error getting temperature for {month} at {lat}, {lon}: {response.text}")
            return None

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def collect_region_temperatures(regions_file: str, locations_file: str, output_file: str) -> pd.DataFrame:
    """
    Collect temperature data for all regions and months, and save to CSV.
    
    Args:
        regions_file (str): Path to the regions CSV file
        locations_file (str): Path to the locations CSV with lat/lon data
        output_file (str): Path to save the output CSV with temperature data
        
    Returns:
        DataFrame: DataFrame containing regions with temperature data
    """
    # Load CSV files into pandas DataFrames
    regions_df = pd.read_csv(regions_file)
    locations_df = pd.read_csv(locations_file)

    # Calculate the average latitude and longitude for each region
    avg_lat_lon_df = locations_df.groupby('new_region')[['lat', 'lon']].mean().reset_index()

    # Months to collect temperature data for
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']

    print("\nStarting temperature collection...")
    for month in months:
        print(f"\nProcessing {month}...")
        avg_temps = []
        for index, row in avg_lat_lon_df.iterrows():
            print(f"Getting temperature for {row['new_region']}...")
            avg_temp = get_avg_temp(row['lat'], row['lon'], month)
            avg_temps.append(avg_temp)
            time.sleep(0.5)  # Add a small delay between API calls
        avg_lat_lon_df[f'water_temp_{month.lower()}'] = avg_temps

    # Merge the average temperature data with the regions DataFrame
    regions_df = regions_df.merge(
        avg_lat_lon_df.drop(columns=['lat', 'lon']),
        on='new_region',
        how='left'
    )

    # Save the modified regions DataFrame to a new CSV file
    regions_df.to_csv(output_file, index=False)
    print("\nSaved data to", output_file)
    
    return regions_df


def add_temperatures_to_database(temperatures_file: str, db_path: str) -> bool:
    """
    Add temperature data to an SQLite database.
    
    Args:
        temperatures_file (str): Path to the CSV file with temperature data
        db_path (str): Path to the SQLite database
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Read the temperature data
        regions_df = pd.read_csv(temperatures_file)
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Months to process
        months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']

        # Add new temperature columns to the existing table if they don't exist
        for month in months:
            column_name = f'water_temp_{month.lower()}'
            try:
                cursor.execute(f'ALTER TABLE regions ADD COLUMN "{column_name}" REAL')
            except sqlite3.OperationalError:
                print(f"Column {column_name} already exists")

        # Update the data in the database
        for _, row in regions_df.iterrows():
            update_query = f'''
            UPDATE regions
            SET {', '.join(f'"{f"water_temp_{month.lower()}"}" = ?' for month in months)}
            WHERE new_region = ?
            '''
            values = [row[f'water_temp_{month.lower()}'] for month in months] + [row['new_region']]
            cursor.execute(update_query, values)

        conn.commit()
        conn.close()
        print("Database updated successfully!")
        return True
    
    except Exception as e:
        print(f"Error updating database: {str(e)}")
        return False


def correlate_temp_with_surf_quality(surf_data_file: str, regions_temp_file: str, output_file: str) -> Dict[str, Any]:
    """
    Analyze correlation between water temperature and surf quality metrics.
    
    Args:
        surf_data_file (str): Path to the surf data CSV
        regions_temp_file (str): Path to the regions temperature CSV
        output_file (str): Path to save the correlation report CSV
        
    Returns:
        dict: Dictionary with correlation analysis results
    """
    # Load the data
    surf_df = pd.read_csv(surf_data_file)
    regions_df = pd.read_csv(regions_temp_file)
    
    # Extract temperature columns
    temp_columns = [col for col in regions_df.columns if col.startswith('water_temp_')]
    
    # Create a mapping from region and month to temperature
    temp_map = {}
    for _, row in regions_df.iterrows():
        region = row['new_region']
        for month in ['january', 'february', 'march', 'april', 'may', 'june',
                     'july', 'august', 'september', 'october', 'november', 'december']:
            temp_col = f'water_temp_{month}'
            if temp_col in row:
                temp_map[(region, month.capitalize())] = row[temp_col]
    
    # Add temperature to surf data
    surf_df['water_temp'] = surf_df.apply(
        lambda row: temp_map.get((row['new_region'], row['month']), np.nan), 
        axis=1
    )
    
    # Calculate correlations
    correlations = []
    
    # For each surf quality metric
    for metric in ['clean', 'blown_out', 'too_small', 'beginner', 'intermediate', 'advanced']:
        if metric in surf_df.columns:
            # Calculate overall correlation
            corr = surf_df['water_temp'].corr(surf_df[metric])
            
            # Calculate correlation by region
            region_corrs = {}
            for region in surf_df['new_region'].unique():
                region_df = surf_df[surf_df['new_region'] == region]
                if len(region_df) >= 10:  # Only calculate if we have enough data points
                    region_corrs[region] = region_df['water_temp'].corr(region_df[metric])
            
            correlations.append({
                'metric': metric,
                'overall_correlation': corr,
                'region_correlations': region_corrs,
                'max_region_corr': max(region_corrs.values()) if region_corrs else np.nan,
                'min_region_corr': min(region_corrs.values()) if region_corrs else np.nan
            })
    
    # Save results
    results_df = pd.DataFrame([{
        'metric': c['metric'],
        'overall_correlation': c['overall_correlation'],
        'max_region_correlation': c['max_region_corr'],
        'min_region_correlation': c['min_region_corr']
    } for c in correlations])
    
    results_df.to_csv(output_file, index=False)
    
    return {
        'correlations': correlations,
        'output_file': output_file
    }
