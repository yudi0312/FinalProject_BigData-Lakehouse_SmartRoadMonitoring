"""
Feature Standardization Service for Silver Layer
Standardizes names, categories, units, and formats across datasets.
"""
import logging
import re
from typing import Optional, Dict, Any

from config import (
    DISTRICT_MAPPING,
    DAMAGE_TYPE_MAPPING,
    WEATHER_CONDITION_MAPPING,
    SEVERITY_LEVEL_MAPPING,
    CONGESTION_LEVEL_MAPPING,
)

logger = logging.getLogger(__name__)


class StandardizationService:
    """
    Service untuk standardisasi feature/kolom di Silver Layer.
    Melakukan normalisasi nama wilayah, kategori, satuan, dll.
    """

    @staticmethod
    def standardize_district_name(district: Optional[str]) -> Optional[str]:
        """
        Standardisasi nama district/wilayah.
        
        Args:
            district: District name (bisa dari berbagai format)
            
        Returns:
            Standardized district name atau None jika invalid
        """
        if not district or not isinstance(district, str):
            return None
        
        # Normalize: lowercase, remove extra spaces, replace underscore/dash
        normalized = district.strip().lower()
        normalized = re.sub(r'[_\-\s]+', '_', normalized)
        
        # Check mapping
        if normalized in DISTRICT_MAPPING:
            return DISTRICT_MAPPING[normalized]
        
        # Try fuzzy match dengan existing mapping
        for key, value in DISTRICT_MAPPING.items():
            if key.replace('_', ' ').lower() == district.strip().lower():
                return value
        
        # Return as title case jika tidak ditemukan di mapping
        logger.warning(f"District '{district}' not in mapping, returning as title case")
        return district.strip().title()

    @staticmethod
    def standardize_damage_type(damage_type: Optional[str]) -> str:
        """
        Standardisasi tipe kerusakan jalan.
        
        Args:
            damage_type: Damage type string
            
        Returns:
            Standardized damage type atau "Unknown"
        """
        if not damage_type or not isinstance(damage_type, str):
            return "Unknown"
        
        # Normalize
        normalized = damage_type.strip().lower()
        normalized = normalized.replace(" ", "_").replace("-", "_")
        
        # Check mapping
        if normalized in DAMAGE_TYPE_MAPPING:
            return DAMAGE_TYPE_MAPPING[normalized]
        
        # Try partial match
        for key, value in DAMAGE_TYPE_MAPPING.items():
            if key in normalized or normalized in key:
                return value
        
        logger.warning(f"Damage type '{damage_type}' not in mapping")
        return "Unknown"

    @staticmethod
    def standardize_severity_level(severity: Optional[Any]) -> str:
        """
        Standardisasi tingkat keparahan (severity).
        Bisa menerima numeric score atau text level.
        
        Args:
            severity: Severity score (int 0-100) atau level (str: low/medium/high/critical)
            
        Returns:
            Standardized severity level: Low/Medium/High/Critical
        """
        if severity is None:
            return "Unknown"
        
        # Jika numeric score
        if isinstance(severity, (int, float)):
            try:
                score = float(severity)
                if score <= 25:
                    return "Low"
                elif score <= 50:
                    return "Medium"
                elif score <= 75:
                    return "High"
                else:
                    return "Critical"
            except (ValueError, TypeError):
                return "Unknown"
        
        # Jika text level
        if isinstance(severity, str):
            normalized = severity.strip().lower()
            if normalized in SEVERITY_LEVEL_MAPPING:
                return SEVERITY_LEVEL_MAPPING[normalized]
            
            # Try fuzzy match
            for key, value in SEVERITY_LEVEL_MAPPING.items():
                if key in normalized or normalized in key:
                    return value
        
        return "Unknown"

    @staticmethod
    def standardize_weather_condition(condition: Optional[str]) -> str:
        """
        Standardisasi kondisi cuaca.
        
        Args:
            condition: Weather condition string
            
        Returns:
            Standardized weather condition
        """
        if not condition or not isinstance(condition, str):
            return "Unknown"
        
        normalized = condition.strip().lower()
        
        # Check mapping
        if normalized in WEATHER_CONDITION_MAPPING:
            return WEATHER_CONDITION_MAPPING[normalized]
        
        # Try partial match
        for key, value in WEATHER_CONDITION_MAPPING.items():
            if key in normalized or normalized in key:
                return value
        
        logger.warning(f"Weather condition '{condition}' not in mapping")
        return condition.strip().title()

    @staticmethod
    def standardize_congestion_level(congestion: Optional[str]) -> str:
        """
        Standardisasi tingkat kemacetan.
        
        Args:
            congestion: Congestion level string
            
        Returns:
            Standardized congestion level
        """
        if not congestion or not isinstance(congestion, str):
            return "Unknown"
        
        normalized = congestion.strip().lower()
        
        # Check mapping
        if normalized in CONGESTION_LEVEL_MAPPING:
            return CONGESTION_LEVEL_MAPPING[normalized]
        
        # Try partial match
        for key, value in CONGESTION_LEVEL_MAPPING.items():
            if key in normalized or normalized in key:
                return value
        
        logger.warning(f"Congestion level '{congestion}' not in mapping")
        return congestion.strip().title()

    @staticmethod
    def standardize_column_names(column_name: str) -> str:
        """
        Standardisasi nama kolom ke snake_case.
        
        Args:
            column_name: Original column name
            
        Returns:
            Snake case column name
        """
        if not column_name:
            return column_name
        
        # Ganti space dan dash dengan underscore
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @staticmethod
    def get_standardization_rules() -> Dict[str, Dict[str, str]]:
        """
        Get semua standardization rules untuk reference.
        
        Returns:
            Dictionary of standardization mappings
        """
        return {
            "districts": DISTRICT_MAPPING,
            "damage_types": DAMAGE_TYPE_MAPPING,
            "weather_conditions": WEATHER_CONDITION_MAPPING,
            "severity_levels": SEVERITY_LEVEL_MAPPING,
            "congestion_levels": CONGESTION_LEVEL_MAPPING,
        }


# Spark SQL functions untuk standardization
def create_standardization_udfs():
    """
    Create Spark UDFs untuk standardization di Spark jobs.
    
    Returns:
        Dictionary of UDFs
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    return {
        "standardize_district": udf(
            lambda x: StandardizationService.standardize_district_name(x),
            StringType()
        ),
        "standardize_damage_type": udf(
            lambda x: StandardizationService.standardize_damage_type(x),
            StringType()
        ),
        "standardize_severity_level": udf(
            lambda x: StandardizationService.standardize_severity_level(x),
            StringType()
        ),
        "standardize_weather_condition": udf(
            lambda x: StandardizationService.standardize_weather_condition(x),
            StringType()
        ),
        "standardize_congestion_level": udf(
            lambda x: StandardizationService.standardize_congestion_level(x),
            StringType()
        ),
    }
