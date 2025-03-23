"""
Surf Data Analysis Main Module

This is the main entry point for the surf data analysis system.
It coordinates the different modules to extract, process, and analyze surf data.
"""

import os
import argparse
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# Import modules
from extractors.gif_extractor import extract_surf_data_from_url
from processors.spot_analyzer import analyze_surf_spots, analyze_missing_spots
from processors.data_merger import merge_csvs, correct_and_merge_data
from analysis.skill_calculator import add_skill_levels, generate_skill_level_report
from utils.missing_finder import find_missing_locations, verify_and_add_ids
from utils.url_builder import SurfGIFFinder


def setup_directories() -> Dict[str, str]:
    """
    Set up the necessary directories for the project.
    
    Returns:
        dict: Dictionary with directory paths
    """
    # Define directories
    directories = {
        'data': 'data',
        'raw': 'data/raw',
        'processed': 'data/processed',
        'reports': 'data/reports',
        'debug': 'debug_images'
    }
    
    # Create directories if they don't exist
    for name, path in directories.items():
        os.makedirs(path, exist_ok=True)
        print(f"Directory '{path}' is ready")
    
    return directories


def extract_data(locations_file: str, base_url: str, output_dir: str, limit: int = 100, start: int = 0) -> str:
    """
    Extract surf data for spots in the locations file.
    
    Args:
        locations_file (str): Path to the locations CSV file
        base_url (str): Base URL for the surf data images
        output_dir (str): Directory to save output files
        limit (int): Maximum number of spots to process
        start (int): Starting index for spot processing
        
    Returns:
        str: Path to the output file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"surf_analysis_{timestamp}.csv")
    
    print(f"Starting data extraction with {limit} spots from index {start}")
    
    # Run the analysis
    results = analyze_surf_spots(
        csv_path=locations_file,
        base_url=base_url,
        output_path=output_file,
        limit=limit,
        start=start
    )
    
    print(f"Extracted data for {len(results)} spot-month combinations")
    
    return output_file


def process_missing_spots(locations_file: str, output_dir: str, 
                         missing_spots_file: str = None) -> str:
    """
    Find and process missing spots.
    
    Args:
        locations_file (str): Path to the locations CSV file
        output_dir (str): Directory to save output files
        missing_spots_file (str, optional): Path to already identified missing spots
        
    Returns:
        str: Path to the output file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # If missing spots file not provided, find missing spots
    if missing_spots_file is None:
        print("Finding missing spots...")
        all_surf_files = [f for f in os.listdir(output_dir) if f.startswith("surf_analysis_") and f.endswith(".csv")]
        
        if not all_surf_files:
            print("No surf analysis files found. Please run extract_data first.")
            return None
        
        # Use the latest surf analysis file
        latest_file = max(all_surf_files, key=lambda x: os.path.getmtime(os.path.join(output_dir, x)))
        combined_file = os.path.join(output_dir, latest_file)
        
        # Find missing locations
        find_missing_locations(combined_file, locations_file)
        
        # Use the generated missing_locations.csv
        missing_spots_file = "missing_locations.csv"
    
    # Check if missing spots file exists
    if not os.path.exists(missing_spots_file):
        print(f"Missing spots file {missing_spots_file} not found.")
        return None
    
    # Create URL finder to find working URLs for missing spots
    finder = SurfGIFFinder(delay=1.0)
    finder.process_missing_locations(missing_spots_file)
    
    # Output file for missing spots analysis
    output_file = os.path.join(output_dir, f"missing_spots_analysis_{timestamp}.csv")
    
    # Analyze missing spots
    merge_file = os.path.join(output_dir, "merged_missing_results.csv")
    results = analyze_missing_spots(
        csv_path=missing_spots_file,
        new_merge_path=merge_file,
        output_path=output_file
    )
    
    print(f"Processed {len(results)} missing spot-month combinations")
    
    return output_file


def merge_all_data(data_dir: str, reference_file: str) -> str:
    """
    Merge all surf data files and add skill levels.
    
    Args:
        data_dir (str): Directory containing surf data files
        reference_file (str): Path to the reference locations file
        
    Returns:
        str: Path to the final merged file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Merge all CSV files in the data directory
    merged_file = os.path.join(data_dir, f"combined_surf_analysis_{timestamp}.csv")
    merged_df = merge_csvs(data_dir, merged_file)
    
    if merged_df is None or len(merged_df) == 0:
        print("No data to merge.")
        return None
    
    # Add IDs to the merged data
    with_ids_file = os.path.join(data_dir, f"surf_data_with_ids_{timestamp}.csv")
    df_with_ids = verify_and_add_ids(merged_file, reference_file, with_ids_file)
    
    # Add skill levels
    final_file = os.path.join(data_dir, f"final_surf_data_{timestamp}.csv")
    final_df = add_skill_levels(with_ids_file, final_file)
    
    print(f"Created final data file with {len(final_df)} rows")
    
    return final_file


def generate_reports(final_data_file: str, reports_dir: str) -> Dict[str, str]:
    """
    Generate various reports from the final data.
    
    Args:
        final_data_file (str): Path to the final processed data file
        reports_dir (str): Directory to save reports
        
    Returns:
        dict: Dictionary with paths to generated reports
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports = {}
    
    # Generate skill level report
    skill_report_file = os.path.join(reports_dir, f"skill_level_report_{timestamp}.csv")
    report_stats = generate_skill_level_report(final_data_file, skill_report_file)
    reports['skill_level'] = skill_report_file
    
    print(f"Generated skill level report with {report_stats['regions_analyzed']} regions")
    
    # TODO: Add more report generation functions here
    
    return reports


def main():
    """
    Main function to run the surf data analysis system.
    """
    parser = argparse.ArgumentParser(description='Surf Data Analysis System')
    
    parser.add_argument('--mode', choices=['extract', 'process_missing', 'merge', 'report', 'full'],
                       default='full', help='Operation mode')
    
    parser.add_argument('--locations', type=str, default='data/locations.csv',
                       help='Path to locations CSV file')
    
    parser.add_argument('--base-url', type=str, 
                       default='https://example.com/data',
                       help='Base URL for surf data images')
    
    parser.add_argument('--limit', type=int, default=100,
                       help='Maximum number of spots to process')
    
    parser.add_argument('--start', type=int, default=0,
                       help='Starting index for spot processing')
    
    parser.add_argument('--missing', type=str, default=None,
                       help='Path to missing spots file')
    
    args = parser.parse_args()
    
    # Setup directories
    dirs = setup_directories()
    
    if args.mode == 'extract' or args.mode == 'full':
        extract_file = extract_data(
            args.locations, 
            args.base_url,
            dirs['raw'],
            args.limit,
            args.start
        )
        print(f"Extraction complete. Output saved to {extract_file}")
    
    if args.mode == 'process_missing' or args.mode == 'full':
        missing_file = process_missing_spots(
            args.locations,
            dirs['raw'],
            args.missing
        )
        if missing_file:
            print(f"Missing spots processing complete. Output saved to {missing_file}")
    
    if args.mode == 'merge' or args.mode == 'full':
        final_file = merge_all_data(dirs['raw'], args.locations)
        if final_file:
            print(f"Data merging complete. Final file saved to {final_file}")
    
    if args.mode == 'report' or args.mode == 'full':
        # Find the latest final data file
        all_final_files = [f for f in os.listdir(dirs['processed']) 
                         if f.startswith("final_surf_data_") and f.endswith(".csv")]
        
        if all_final_files:
            latest_file = max(all_final_files, key=lambda x: os.path.getmtime(os.path.join(dirs['processed'], x)))
            final_file = os.path.join(dirs['processed'], latest_file)
            
            reports = generate_reports(final_file, dirs['reports'])
            print(f"Report generation complete. Reports saved to {dirs['reports']}")
        else:
            print("No final data file found. Please run merge mode first.")
    
    print("Surf Data Analysis complete!")


if __name__ == "__main__":
    main()
