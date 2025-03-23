# Surf Data Analysis Project

## Overview
This project extracts, processes, and analyzes surf conditions data from various surf spots around the world. The system extracts data from surf forecast graphs, processes the information, and creates a comprehensive database of surf conditions including wave heights, cleanliness ratings, and suitability for different skill levels.

## Features
- Automated OCR-based extraction of surf consistency data from images
- Wave height analysis and classification
- Surf conditions categorization (clean, blown out, too small)
- Skill level suitability calculation (beginner, intermediate, advanced)
- Data validation and completion checking
- Regional surf analytics

## Structure
- `src/extractors/` - Contains modules for data extraction
- `src/processors/` - Data cleaning and processing utilities
- `src/analysis/` - Analytics and calculation modules
- `src/utils/` - Helper functions and utilities
- `data/` - Placeholder for processed data (not included in repo)

## Technical Details
This project utilizes:
- Python for data processing and analysis
- OCR (Optical Character Recognition) via Tesseract
- Pandas for data manipulation and analysis
- PIL (Python Imaging Library) for image processing
- RegEx for pattern matching and text extraction
- SQLite for data storage

## Getting Started

### Prerequisites
- Python 3.8+
- Tesseract OCR
- Required Python packages (see requirements.txt)

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/surf-data-analysis.git
cd surf-data-analysis

# Install Tesseract OCR (Ubuntu/Debian)
apt-get install -y tesseract-ocr

# Install Python dependencies
pip install -r requirements.txt
```

### Usage
1. Configure your spot locations in a CSV file
2. Run the data extractor to process surf condition images
3. Analyze the results with the provided analytics tools

```python
# Example usage
from src.extractors.gif_extractor import extract_surf_data_from_url
from src.processors.data_processor import process_surf_data

# Extract data for a specific spot and month
data = extract_surf_data_from_url(url, spot_name, month)

# Process the extracted data
processed_data = process_surf_data(data)
```

## Future Improvements
- Web interface for data visualization
- Predictive models for surf forecasting
- API for accessing processed data
- Mobile application integration

## License
[MIT License](LICENSE)
