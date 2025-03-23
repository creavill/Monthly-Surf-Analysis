"""
Surf Data Analysis Package

This package contains modules for analyzing surf data and generating insights.
"""

from .skill_calculator import (
    add_skill_levels,
    calculate_best_months,
    rank_spots_by_skill_level,
    generate_skill_level_report
)

__all__ = [
    'add_skill_levels',
    'calculate_best_months',
    'rank_spots_by_skill_level',
    'generate_skill_level_report'
]
