"""
Skill Level Calculator Module

This module calculates suitability scores for different surf skill levels
(beginner, intermediate, advanced) based on wave height data and conditions.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any


def add_skill_levels(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Calculate suitability percentages for beginner, intermediate, and advanced surfers.
    
    Args:
        input_file (str): Path to the input CSV file with surf data
        output_file (str): Path to save the processed output CSV file
        
    Returns:
        DataFrame: The processed dataframe with added skill level columns
    """
    # Read the combined CSV file
    df = pd.read_csv(input_file)
    
    # Ensure numeric data types for calculations
    numeric_cols = ['clean', 'blown_out', 'too_small', 'flat', 
                   'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate suitability percentages for each skill level
    
    # Beginners: primarily 0-4ft waves
    # Beginners prefer smaller waves and very clean conditions
    df['beginner'] = df['height_0_4']
    
    # Intermediates: primarily 4-6ft waves plus some smaller waves
    # Intermediates can handle a mix of wave sizes
    df['intermediate'] = df['height_4_6'] + (df['height_0_4'] * 0.5)
    
    # Advanced: 6ft+ waves plus most of the 4-6ft waves
    # Advanced surfers prefer larger waves and can handle most medium waves
    df['advanced'] = df['height_6_10'] + df['height_10_plus'] + (df['height_4_6'] * 0.8)
    
    # Additional step: factor in "clean" percentage
    # Better surfers can handle worse conditions
    df['beginner'] = df['beginner'] * (df['clean'] / 100)
    df['intermediate'] = df['intermediate'] * ((df['clean'] + 15) / 100)  # Can handle slightly worse conditions
    df['advanced'] = df['advanced'] * ((df['clean'] + 25) / 100)  # Can handle even worse conditions
    
    # Cap values at 100
    df[['beginner', 'intermediate', 'advanced']] = df[['beginner', 'intermediate', 'advanced']].clip(0, 100)
    
    # Round to 1 decimal place
    df[['beginner', 'intermediate', 'advanced']] = df[['beginner', 'intermediate', 'advanced']].round(1)
    
    # Save the result
    df.to_csv(output_file, index=False)
    
    return df


def calculate_best_months(df: pd.DataFrame, skill_level: str = 'intermediate') -> Dict[str, Any]:
    """
    Calculate the best months for each region based on a specific skill level.
    
    Args:
        df (DataFrame): DataFrame containing surf data with skill level columns
        skill_level (str): The skill level to analyze ('beginner', 'intermediate', or 'advanced')
        
    Returns:
        dict: Dictionary with regions as keys and their best months as values
    """
    if skill_level not in ['beginner', 'intermediate', 'advanced']:
        raise ValueError("skill_level must be one of: 'beginner', 'intermediate', 'advanced'")
        
    # Group by region and month, calculate the average score for the specified skill level
    region_month_avg = df.groupby(['new_region', 'month'])[skill_level].mean().reset_index()
    
    # For each region, find the month with the highest score
    best_months = {}
    for region in region_month_avg['new_region'].unique():
        region_data = region_month_avg[region_month_avg['new_region'] == region]
        best_month_idx = region_data[skill_level].idxmax()
        best_month = region_data.loc[best_month_idx, 'month']
        best_score = region_data.loc[best_month_idx, skill_level]
        
        best_months[region] = {
            'best_month': best_month,
            'score': best_score
        }
        
    return best_months


def rank_spots_by_skill_level(df: pd.DataFrame, skill_level: str = 'intermediate', 
                             month: str = None, region: str = None, top_n: int = 10) -> pd.DataFrame:
    """
    Rank spots by their suitability for a specific skill level.
    
    Args:
        df (DataFrame): DataFrame containing surf data with skill level columns
        skill_level (str): The skill level to analyze ('beginner', 'intermediate', or 'advanced')
        month (str, optional): Filter for a specific month
        region (str, optional): Filter for a specific region
        top_n (int): Number of top spots to return
        
    Returns:
        DataFrame: DataFrame with the top spots for the specified criteria
    """
    if skill_level not in ['beginner', 'intermediate', 'advanced']:
        raise ValueError("skill_level must be one of: 'beginner', 'intermediate', 'advanced'")
    
    # Apply filters if provided
    filtered_df = df.copy()
    
    if month:
        filtered_df = filtered_df[filtered_df['month'] == month]
        
    if region:
        filtered_df = filtered_df[filtered_df['new_region'] == region]
    
    # Sort by the specified skill level score (descending)
    ranked_df = filtered_df.sort_values(by=skill_level, ascending=False)
    
    # Get the top N spots
    top_spots = ranked_df.head(top_n)
    
    # Select relevant columns for display
    result_cols = ['name', 'new_region', 'month', skill_level, 'clean', 
                   'height_0_4', 'height_4_6', 'height_6_10', 'height_10_plus']
    
    return top_spots[result_cols]


def generate_skill_level_report(input_file: str, output_file: str) -> Dict[str, Any]:
    """
    Generate a comprehensive report on skill level suitability across regions and months.
    
    Args:
        input_file (str): Path to the input CSV file with surf data including skill levels
        output_file (str): Path to save the output report in CSV format
        
    Returns:
        dict: Dictionary with summary statistics from the report
    """
    # Read the input file
    df = pd.read_csv(input_file)
    
    # Initialize report data structure
    report_data = []
    
    # Calculate global statistics
    global_stats = {
        'avg_beginner': df['beginner'].mean(),
        'avg_intermediate': df['intermediate'].mean(),
        'avg_advanced': df['advanced'].mean(),
        'max_beginner': df['beginner'].max(),
        'max_intermediate': df['intermediate'].max(),
        'max_advanced': df['advanced'].max()
    }
    
    # For each region
    for region in df['new_region'].unique():
        region_df = df[df['new_region'] == region]
        
        # Calculate regional statistics
        region_stats = {
            'region': region,
            'avg_beginner': region_df['beginner'].mean(),
            'avg_intermediate': region_df['intermediate'].mean(),
            'avg_advanced': region_df['advanced'].mean()
        }
        
        # Find best months for each skill level in this region
        for skill in ['beginner', 'intermediate', 'advanced']:
            month_avgs = region_df.groupby('month')[skill].mean()
            best_month = month_avgs.idxmax()
            best_score = month_avgs.max()
            
            region_stats[f'best_{skill}_month'] = best_month
            region_stats[f'best_{skill}_score'] = best_score
        
        report_data.append(region_stats)
    
    # Convert to DataFrame and save
    report_df = pd.DataFrame(report_data)
    report_df.to_csv(output_file, index=False)
    
    # Return summary
    return {
        'regions_analyzed': len(report_data),
        'global_stats': global_stats,
        'report_file': output_file
    }
