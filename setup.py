from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="surf-data-analysis",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A system for extracting and analyzing surf data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/surf-data-analysis",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.25.1",
        "Pillow>=8.2.0",
        "pytesseract>=0.3.8",
        "pandas>=1.2.4",
        "numpy>=1.20.3",
        "tqdm>=4.61.0",
        "python-dateutil>=2.8.1",
    ],
    entry_points={
        "console_scripts": [
            "surf-analysis=src.main:main",
        ],
    },
)
