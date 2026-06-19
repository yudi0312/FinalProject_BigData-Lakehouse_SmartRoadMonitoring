"""
Geocoding Service for Silver Layer
Converts addresses/locations to latitude and longitude coordinates.
Uses district-based geocoding with fallback to Surabaya center.
"""
import logging
from typing import Optional, Tuple

from config import DISTRICT_COORDINATES, SURABAYA_DEFAULT_COORDS

logger = logging.getLogger(__name__)


class GeocodingService:
    """
    Service untuk mengkonversi alamat/lokasi ke koordinat latitude dan longitude.
    Menggunakan data district yang sudah tersedia.
    """

    @staticmethod
    def geocode_by_district(district: str) -> Tuple[float, float]:
        """
        Geocode berdasarkan district name.
        Returns tuple of (latitude, longitude)
        
        Args:
            district: District name
            
        Returns:
            Tuple of (latitude, longitude) or Surabaya center if not found
        """
        if not district or not isinstance(district, str):
            logger.warning(f"Invalid district: {district}")
            return SURABAYA_DEFAULT_COORDS["latitude"], SURABAYA_DEFAULT_COORDS["longitude"]
        
        # Normalize district name
        normalized_district = district.strip().title()
        
        # Try exact match first
        if normalized_district in DISTRICT_COORDINATES:
            coords = DISTRICT_COORDINATES[normalized_district]
            logger.debug(f"Found exact match for {normalized_district}: {coords}")
            return coords
        
        # Try case-insensitive match
        for known_district, coords in DISTRICT_COORDINATES.items():
            if known_district.lower() == normalized_district.lower():
                logger.debug(f"Found case-insensitive match for {district}: {coords}")
                return coords
        
        # Fallback to Surabaya center
        logger.warning(f"District {district} not found, using Surabaya center")
        return SURABAYA_DEFAULT_COORDS["latitude"], SURABAYA_DEFAULT_COORDS["longitude"]

    @staticmethod
    def geocode(address: Optional[str], district: Optional[str], 
                default_lat: Optional[float] = None, 
                default_long: Optional[float] = None) -> Tuple[float, float]:
        """
        Geocode address dengan fallback ke district-based geocoding.
        
        Args:
            address: Full address string (future: bisa pakai external API)
            district: District name for fallback
            default_lat: Default latitude if geocoding fails
            default_long: Default longitude if geocoding fails
            
        Returns:
            Tuple of (latitude, longitude)
        """
        # Jika sudah ada koordinat, gunakan itu
        if default_lat is not None and default_long is not None:
            return float(default_lat), float(default_long)
        
        # Coba geocode by district
        if district:
            lat, lng = GeocodingService.geocode_by_district(district)
            if lat != SURABAYA_DEFAULT_COORDS["latitude"] or lng != SURABAYA_DEFAULT_COORDS["longitude"]:
                return lat, lng
        
        # Fallback ke Surabaya center
        return SURABAYA_DEFAULT_COORDS["latitude"], SURABAYA_DEFAULT_COORDS["longitude"]

    @staticmethod
    def validate_coordinates(latitude: Optional[float], 
                           longitude: Optional[float],
                           min_lat: float = -7.5,
                           max_lat: float = -7.0,
                           min_long: float = 112.5,
                           max_long: float = 113.0) -> bool:
        """
        Validate if coordinates are within Surabaya bounds.
        
        Args:
            latitude: Latitude value
            longitude: Longitude value
            min_lat: Minimum latitude (southernmost)
            max_lat: Maximum latitude (northernmost)
            min_long: Minimum longitude (westernmost)
            max_long: Maximum longitude (easternmost)
            
        Returns:
            True if coordinates are valid, False otherwise
        """
        if latitude is None or longitude is None:
            return False
        
        try:
            lat = float(latitude)
            lng = float(longitude)
            
            is_valid = (min_lat <= lat <= max_lat) and (min_long <= lng <= max_long)
            
            if not is_valid:
                logger.warning(f"Invalid coordinates: ({lat}, {lng}) outside Surabaya bounds")
            
            return is_valid
        except (TypeError, ValueError) as e:
            logger.error(f"Error validating coordinates: {e}")
            return False

    @staticmethod
    def get_all_districts() -> dict:
        """
        Get all available district coordinates.
        
        Returns:
            Dictionary of district names to (lat, lng) tuples
        """
        return DISTRICT_COORDINATES.copy()

    @staticmethod
    def get_surabaya_bounds() -> dict:
        """
        Get Surabaya geographic bounds.
        
        Returns:
            Dictionary with min/max lat/lng
        """
        return {
            "min_latitude": -7.5,
            "max_latitude": -7.0,
            "min_longitude": 112.5,
            "max_longitude": 113.0,
            "center_latitude": -7.2575,
            "center_longitude": 112.7521,
        }


# Spark UDF wrapper untuk geocoding
def create_geocode_udf():
    """
    Create Spark UDF untuk geocoding di Spark jobs.
    Usage: df.withColumn("latitude", geocode_udf(col("district")))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, DoubleType
    
    def geocode_spark(district: str) -> dict:
        """Spark UDF untuk geocoding"""
        if not district:
            return {
                "latitude": SURABAYA_DEFAULT_COORDS["latitude"],
                "longitude": SURABAYA_DEFAULT_COORDS["longitude"]
            }
        
        lat, lng = GeocodingService.geocode_by_district(district)
        return {"latitude": lat, "longitude": lng}
    
    return_type = StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ])
    
    return udf(geocode_spark, returnType=return_type)
