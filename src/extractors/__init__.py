"""
Surf Data Extractors Package

This package contains modules for extracting surf data from various sources.
"""

from .gif_extractor import (
    extract_surf_data_from_url,
    extract_text_from_url_gif,
    clean_gif_text
)

__all__ = [
    'extract_surf_data_from_url',
    'extract_text_from_url_gif',
    'clean_gif_text'
]
