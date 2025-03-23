"""
Surf Data GIF Extractor Module

This module provides functionality to extract surf data from GIF images
using OCR (Optical Character Recognition). It processes surf condition images 
to extract values for wave heights, cleanliness, and other attributes.
"""

import requests
from PIL import Image, ImageEnhance
from io import BytesIO
import re
import os
import pytesseract
from typing import Dict, Optional, List, Any


def clean_percentage(text: str) -> float:
    """
    Helper function to clean and convert percentage values.
    
    Args:
        text (str): Raw text containing percentage value
        
    Returns:
        float: Cleaned percentage value
    """
    try:
        # Replace common OCR mistakes
        text = text.replace('O', '0').replace('o', '0')
        return float(text)
    except (ValueError, TypeError):
        return 0.0


def save_debug_image(img: Image.Image, name: str, spot_name: str, debug_dir: str = "debug_images") -> None:
    """
    Save debug images in a structured folder for QA and analysis.
    
    Args:
        img (Image.Image): PIL Image object to save
        name (str): Filename to use
        spot_name (str): Name of the surf spot
        debug_dir (str): Base directory for debug images
    """
    spot_dir = os.path.join(debug_dir, spot_name)
    if not os.path.exists(spot_dir):
        os.makedirs(spot_dir)
    img.save(os.path.join(spot_dir, name))


def extract_surf_data_from_url(url: str, spot_name: str, month: str) -> Optional[Dict[str, Any]]:
    """
    Extract surf data from a given URL containing a surf condition GIF.
    
    This function:
    1. Downloads the image from the URL
    2. Processes it to enhance readability for OCR
    3. Extracts specific data regions (top section for condition percentages, 
       bottom section for wave height bars)
    4. Uses OCR to extract text values
    5. Parses the extracted text into structured data
    
    Args:
        url (str): URL of the surf condition image
        spot_name (str): Name of the surf spot
        month (str): Month of the data
        
    Returns:
        dict: Extracted surf data or None if extraction failed
    """
    try:
        # Fetch the image
        response = requests.get(url)
        if response.status_code != 200:
            return None

        # Open and process image
        img = Image.open(BytesIO(response.content))

        # Resize and enhance image for better OCR results
        width, height = img.size
        img = img.resize((width*4, height*4), Image.Resampling.LANCZOS)
        if img.mode != 'RGB':
            img = img.convert('RGB')
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(2.0)

        # Create crops for different data sections
        top_crop = img.crop((0, 0, width*4, height*4 * 0.2))
        bottom_crop = img.crop((0, height*4 * 0.7, width*4, height*4))

        # Split bottom into bars (wave height categories)
        bar_width = bottom_crop.width // 5
        bar_crops = []
        for i in range(5):
            bar_crop = bottom_crop.crop((i * bar_width, 0, (i + 1) * bar_width, bottom_crop.height))
            bar_crops.append(bar_crop)
            save_debug_image(bar_crop, f"{month}_bar_{i+1}.png", spot_name)

        # Initialize data structure
        surf_data = {
            "location": spot_name,
            "month": month.capitalize(),
            "clean": 0.0,
            "blown_out": 0.0,
            "too_small": 0.0,
            "flat": 0.0,
            "height_0_4": 0.0,
            "height_4_6": 0.0,
            "height_6_10": 0.0,
            "height_10_plus": 0.0
        }

        # Process top section for main percentages (conditions)
        top_text = pytesseract.image_to_string(top_crop, config='--psm 6 -c tessedit_char_whitelist=0123456789.%CleanBowutsmf')

        # Use regex to extract values
        clean_match = re.search(r'Clean\s*(\d+\.?\d*)%', top_text)
        blown_match = re.search(r'(?:Blown|Biown)\s*out\s*(\d+\.?\d*)%', top_text)
        small_match = re.search(r'[Tt]oo\s*small\s*(\d+\.?\d*)%', top_text)

        if clean_match: surf_data["clean"] = float(clean_match.group(1))
        if blown_match: surf_data["blown_out"] = float(blown_match.group(1))
        if small_match: surf_data["too_small"] = float(small_match.group(1))

        # Process each bar for wave heights
        wave_heights = []
        for bar_crop in bar_crops:
            bar_text = pytesseract.image_to_string(bar_crop, config='--psm 3 -c tessedit_char_whitelist=0123456789.%')
            percentages = re.findall(r'(\d+\.?\d*)%', bar_text)
            if percentages:
                try:
                    val = float(percentages[0])
                    wave_heights.append(val if val <= 100 else 0.0)
                except ValueError:
                    wave_heights.append(0.0)
            else:
                wave_heights.append(0.0)

        # Assign wave heights to corresponding columns
        if len(wave_heights) >= 5:
            surf_data["flat"] = wave_heights[0]
            surf_data["height_0_4"] = wave_heights[1]  # 0.5-1.3m / ~2-4ft
            surf_data["height_4_6"] = wave_heights[2]  # 1.3-2m / ~4-6ft
            surf_data["height_6_10"] = wave_heights[3]  # 2-3m / ~6-10ft
            surf_data["height_10_plus"] = wave_heights[4]  # >3m / >10ft

        return surf_data
    except Exception as e:
        print(f"Error processing {spot_name} - {month}: {str(e)}")
        return None


def extract_text_from_url_gif(url: str) -> List[str]:
    """
    Basic function to extract all text from a GIF.
    
    Args:
        url (str): URL of the GIF
        
    Returns:
        list: List of extracted text strings from all frames
    """
    response = requests.get(url)
    gif = Image.open(BytesIO(response.content))
    texts = []

    try:
        while True:
            frame = gif.convert('RGB')
            text = pytesseract.image_to_string(frame)
            if text.strip():
                texts.append(text.strip())
            gif.seek(gif.tell() + 1)
    except EOFError:
        pass

    return texts


def clean_gif_text(texts: List[str]) -> List[str]:
    """
    Clean extracted text from GIFs by removing noise patterns.
    
    Args:
        texts (list): List of raw text strings
        
    Returns:
        list: Cleaned text strings
    """
    clean_texts = []
    for text in texts:
        # Split into lines and remove empty ones
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        # Remove common noise patterns
        lines = [line for line in lines if not any(x in line.lower() for x in ['om', 'gm', 'opt', 'sbt'])]

        # Join with commas
        clean_text = ','.join(lines)
        clean_texts.append(clean_text)

    return clean_texts
