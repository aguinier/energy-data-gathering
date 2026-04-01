"""
ENTSO-E API Client Wrapper

Wraps the entsoe-py library with:
- Rate limiting
- Retry logic with exponential backoff
- Enhanced error handling
- Response validation
"""

import time
import logging
from datetime import datetime
from typing import Optional, Tuple
import pandas as pd
import pytz
import xml.etree.ElementTree as ET
from entsoe import EntsoePandasClient, EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError, InvalidPSRTypeError, PaginationError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config
import utils


logger = logging.getLogger('entsoe_pipeline')


# Bidding zone to country code mapping for multi-zone countries
BIDDING_ZONE_TO_COUNTRY = {
    "DE_AT_LU": "DE", "DE_LU": "DE",
    "IT_NORD": "IT", "IT_CNOR": "IT", "IT_CSUD": "IT", "IT_SUD": "IT",
    "IT_SARD": "IT", "IT_SICI": "IT", "IT_NORD_AT": "IT",
    "IT_NORD_FR": "IT", "IT_NORD_SI": "IT", "IT_NORD_CH": "IT",
    "IT_BRNN": "IT", "IT_FOGN": "IT", "IT_GR": "IT",
    "IT_PRGP": "IT", "IT_ROSN": "IT", "IT_CALA": "IT",
    "DK_1": "DK", "DK_2": "DK",
    "SE_1": "SE", "SE_2": "SE", "SE_3": "SE", "SE_4": "SE",
    "NO_1": "NO", "NO_2": "NO", "NO_3": "NO", "NO_4": "NO", "NO_5": "NO",
    "GB": "GB", "GB_NIR": "GB",
    "IE_SEM": "IE",
}


def normalize_zone_to_country(zone_name: str) -> str:
    """Map an entsoe-py bidding zone name to a 2-letter country code."""
    if zone_name in BIDDING_ZONE_TO_COUNTRY:
        return BIDDING_ZONE_TO_COUNTRY[zone_name]
    if len(zone_name) == 2 and zone_name.isalpha():
        return zone_name
    return zone_name


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ENTSOEClientError(Exception):
    """Base exception for ENTSO-E client errors"""
    pass


class ENTSOERateLimitError(ENTSOEClientError):
    """Raised when API rate limit is exceeded"""
    pass


class ENTSOENoDataError(ENTSOEClientError):
    """Raised when no data is available for the request"""
    pass


# ============================================================================
# ENTSO-E CLIENT
# ============================================================================

class ENTSOEClient:
    """
    Enhanced ENTSO-E API client with rate limiting and retry logic
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize ENTSO-E client

        Args:
            api_key: ENTSO-E API key (defaults to config.ENTSOE_API_KEY)
        """
        self.api_key = api_key or config.ENTSOE_API_KEY

        if not self.api_key:
            raise ValueError("ENTSO-E API key not provided")

        # Initialize entsoe-py clients
        self.client = EntsoePandasClient(api_key=self.api_key)
        self.raw_client = EntsoeRawClient(api_key=self.api_key)

        # Rate limiting
        self.last_request_time = 0
        self.request_delay = config.REQUEST_DELAY_SECONDS

        logger.info("ENTSO-E client initialized")

    def _rate_limit(self):
        """Enforce rate limiting between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        reraise=True
    )
    def _make_request(self, method, *args, **kwargs):
        """
        Make API request with rate limiting and retry logic

        Args:
            method: API method to call
            *args: Positional arguments for method
            **kwargs: Keyword arguments for method

        Returns:
            API response (usually pandas DataFrame or Series)
        """
        self._rate_limit()

        try:
            logger.debug(f"Making API request: {method.__name__}")
            result = method(*args, **kwargs)
            logger.debug(f"Request successful: {method.__name__}")
            return result

        except NoMatchingDataError as e:
            logger.warning(f"No data available: {e}")
            raise ENTSOENoDataError(f"No data available: {e}") from e

        except InvalidPSRTypeError as e:
            logger.error(f"Invalid PSR type: {e}")
            raise ENTSOEClientError(f"Invalid PSR type: {e}") from e

        except PaginationError as e:
            logger.error(f"Pagination error: {e}")
            raise ENTSOEClientError(f"Pagination error: {e}") from e

        except Exception as e:
            logger.error(f"API request failed: {utils.format_error(e)}")
            raise ENTSOEClientError(f"API request failed: {e}") from e

    def _extract_publication_timestamp(self, xml_response: str) -> Optional[datetime]:
        """
        Extract publication timestamp from ENTSO-E XML response

        Args:
            xml_response: Raw XML string from ENTSO-E API

        Returns:
            Publication timestamp in UTC, or None if not found
        """
        try:
            root = ET.fromstring(xml_response)

            # Search for createdDateTime tag (works across all data types)
            for elem in root.iter():
                if 'createdDateTime' in elem.tag:
                    timestamp_str = elem.text
                    # Parse ISO 8601 timestamp
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    return dt

            logger.warning("No publication timestamp found in XML response")
            return None

        except Exception as e:
            logger.error(f"Error extracting publication timestamp: {e}")
            return None

    def _parse_week_ahead_min_max(self, xml_response: str) -> Optional[pd.DataFrame]:
        """
        Parse week-ahead forecast XML to extract min/max values per day.

        ENTSO-E week-ahead forecasts contain multiple TimeSeries with different
        businessType values that indicate min/max forecasts.

        Args:
            xml_response: Raw XML string from ENTSO-E API

        Returns:
            DataFrame with columns: timestamp_utc, forecast_min_mw, forecast_max_mw, forecast_value_mw
            or None if parsing fails
        """
        try:
            root = ET.fromstring(xml_response)

            # Find the namespace from the root tag
            ns_match = root.tag.split('}')[0] + '}' if '}' in root.tag else ''

            min_data = {}  # timestamp -> min value
            max_data = {}  # timestamp -> max value

            # Iterate through all TimeSeries elements
            for ts in root.iter(f'{ns_match}TimeSeries'):
                # Find businessType (A60 = min, A61 = max for week-ahead)
                business_type_elem = ts.find(f'.//{ns_match}businessType')
                business_type = business_type_elem.text if business_type_elem is not None else None

                # Iterate through Period elements
                for period in ts.iter(f'{ns_match}Period'):
                    # Get period start time
                    start_elem = period.find(f'.//{ns_match}start')
                    if start_elem is None:
                        continue

                    # Get resolution (should be P1D for daily)
                    resolution_elem = period.find(f'.//{ns_match}resolution')
                    resolution = resolution_elem.text if resolution_elem is not None else 'PT60M'

                    # Parse each Point
                    for point in period.iter(f'{ns_match}Point'):
                        position_elem = point.find(f'{ns_match}position')
                        quantity_elem = point.find(f'{ns_match}quantity')

                        if position_elem is None or quantity_elem is None:
                            continue

                        position = int(position_elem.text)
                        quantity = float(quantity_elem.text)

                        # Calculate timestamp from start + position
                        start_time = datetime.fromisoformat(start_elem.text.replace('Z', '+00:00'))

                        # For daily resolution (P1D), each position is one day
                        if resolution == 'P1D':
                            from datetime import timedelta
                            timestamp = start_time + timedelta(days=position - 1)
                        else:
                            # For hourly/other resolution, calculate accordingly
                            hours = position - 1
                            from datetime import timedelta
                            timestamp = start_time + timedelta(hours=hours)

                        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

                        # Store based on business type
                        # A60 = Minimum, A61 = Maximum (or try both naming conventions)
                        if business_type in ('A60', 'B10'):  # Minimum forecast
                            min_data[timestamp_str] = quantity
                        elif business_type in ('A61', 'B11'):  # Maximum forecast
                            max_data[timestamp_str] = quantity
                        else:
                            # If no specific type, store as both (single value)
                            if timestamp_str not in min_data:
                                min_data[timestamp_str] = quantity
                            if timestamp_str not in max_data:
                                max_data[timestamp_str] = quantity

            if not min_data and not max_data:
                logger.warning("No min/max data found in week-ahead XML")
                return None

            # Combine into DataFrame
            all_timestamps = sorted(set(min_data.keys()) | set(max_data.keys()))

            df = pd.DataFrame({
                'timestamp_utc': pd.to_datetime(all_timestamps),
                'forecast_min_mw': [min_data.get(t) for t in all_timestamps],
                'forecast_max_mw': [max_data.get(t) for t in all_timestamps],
            })

            # Calculate midpoint as forecast_value_mw
            df['forecast_value_mw'] = (
                df['forecast_min_mw'].fillna(0) + df['forecast_max_mw'].fillna(0)
            ) / 2

            # Where we only have one value, use it as all three
            mask_only_min = df['forecast_max_mw'].isna() & df['forecast_min_mw'].notna()
            mask_only_max = df['forecast_min_mw'].isna() & df['forecast_max_mw'].notna()

            df.loc[mask_only_min, 'forecast_max_mw'] = df.loc[mask_only_min, 'forecast_min_mw']
            df.loc[mask_only_min, 'forecast_value_mw'] = df.loc[mask_only_min, 'forecast_min_mw']

            df.loc[mask_only_max, 'forecast_min_mw'] = df.loc[mask_only_max, 'forecast_max_mw']
            df.loc[mask_only_max, 'forecast_value_mw'] = df.loc[mask_only_max, 'forecast_max_mw']

            logger.info(f"Parsed {len(df)} week-ahead min/max records")
            return df

        except Exception as e:
            logger.error(f"Error parsing week-ahead min/max XML: {e}")
            return None

    # ========================================================================
    # LOAD DATA
    # ========================================================================

    def query_load(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Query actual total load (electricity demand)

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            DataFrame with columns: timestamp_utc, load_mw
            None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Query load data
            series = self._make_request(
                self.client.query_load,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts
            )

            # Convert Series to DataFrame
            if isinstance(series, pd.Series):
                df = series.to_frame(name='load_mw')
                df.index.name = 'timestamp_utc'
                df = df.reset_index()
            else:
                df = series.copy()
                df.columns = ['load_mw']
                df.index.name = 'timestamp_utc'
                df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data
            df = utils.remove_outliers(df, 'load_mw', 'load')

            logger.info(f"Retrieved {len(df)} load records for {country_code}")
            return df

        except ENTSOENoDataError:
            logger.warning(f"No load data for {country_code} ({start} to {end})")
            return None

    def query_load_with_metadata(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Query actual total load with publication timestamp metadata

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            Tuple of (dataframe, publication_timestamp)
            - DataFrame with columns: timestamp_utc, load_mw
            - Publication timestamp when ENTSO-E created the data
            Both are None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Fetch raw XML first to extract publication timestamp
            raw_xml = self._make_request(
                self.raw_client.query_load,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts
            )

            # Extract publication timestamp from XML
            publication_time = self._extract_publication_timestamp(raw_xml)

            # Now get the data using the pandas client (existing logic)
            series = self._make_request(
                self.client.query_load,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts
            )

            # Convert Series to DataFrame
            if isinstance(series, pd.Series):
                df = series.to_frame(name='load_mw')
                df.index.name = 'timestamp_utc'
                df = df.reset_index()
            else:
                df = series.copy()
                df.columns = ['load_mw']
                df.index.name = 'timestamp_utc'
                df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data
            df = utils.remove_outliers(df, 'load_mw', 'load')

            logger.info(f"Retrieved {len(df)} load records for {country_code} (published: {publication_time})")
            return df, publication_time

        except ENTSOENoDataError:
            logger.warning(f"No load data for {country_code} ({start} to {end})")
            return None, None

    # ========================================================================
    # PRICE DATA
    # ========================================================================

    def query_day_ahead_prices(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Query day-ahead electricity prices

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            DataFrame with columns: timestamp_utc, price_eur_mwh
            None if no data available
        """
        try:
            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Check if this is a multi-zone country (NO, SE, DK)
            if self._is_multi_zone_country(country_code):
                df = self._get_multi_zone_prices(country_code, start_ts, end_ts)
                if df is None:
                    return None
            else:
                # Get the correct bidding zone for price data
                # Some countries use different zones for prices vs load
                price_zone = self._get_price_country_code(country_code)

                # Query price data using the bidding zone
                series = self._make_request(
                    self.client.query_day_ahead_prices,
                    price_zone,
                    start=start_ts,
                    end=end_ts
                )

                # Convert Series to DataFrame
                if isinstance(series, pd.Series):
                    df = series.to_frame(name='price_eur_mwh')
                    df.index.name = 'timestamp_utc'
                    df = df.reset_index()
                else:
                    df = series.copy()
                    df.columns = ['price_eur_mwh']
                    df.index.name = 'timestamp_utc'
                    df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data (allow negative prices - they occur with high renewables)
            df = utils.remove_outliers(df, 'price_eur_mwh', 'price')

            logger.info(f"Retrieved {len(df)} price records for {country_code}")
            return df

        except ENTSOENoDataError:
            logger.warning(f"No price data for {country_code} ({start} to {end})")
            return None

    def query_day_ahead_prices_with_metadata(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Query day-ahead electricity prices with publication timestamp metadata

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            Tuple of (dataframe, publication_timestamp)
            - DataFrame with columns: timestamp_utc, price_eur_mwh
            - Publication timestamp when ENTSO-E created the data
            Both are None if no data available
        """
        try:
            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Check if this is a multi-zone country (NO, SE, DK)
            if self._is_multi_zone_country(country_code):
                df = self._get_multi_zone_prices(country_code, start_ts, end_ts)
                if df is None:
                    return None, None
                # For multi-zone countries, use current time as publication time
                # (we can't easily aggregate publication times from multiple zones)
                publication_time = datetime.now(pytz.UTC)
            else:
                # Get the correct bidding zone for price data
                price_zone = self._get_price_country_code(country_code)

                # Fetch raw XML first to extract publication timestamp
                raw_xml = self._make_request(
                    self.raw_client.query_day_ahead_prices,
                    price_zone,
                    start=start_ts,
                    end=end_ts
                )

                # Extract publication timestamp from XML
                publication_time = self._extract_publication_timestamp(raw_xml)

                # Now get the data using the pandas client (existing logic)
                series = self._make_request(
                    self.client.query_day_ahead_prices,
                    price_zone,
                    start=start_ts,
                    end=end_ts
                )

                # Convert Series to DataFrame
                if isinstance(series, pd.Series):
                    df = series.to_frame(name='price_eur_mwh')
                    df.index.name = 'timestamp_utc'
                    df = df.reset_index()
                else:
                    df = series.copy()
                    df.columns = ['price_eur_mwh']
                    df.index.name = 'timestamp_utc'
                    df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data (allow negative prices - they occur with high renewables)
            df = utils.remove_outliers(df, 'price_eur_mwh', 'price')

            logger.info(f"Retrieved {len(df)} price records for {country_code} (published: {publication_time})")
            return df, publication_time

        except ENTSOENoDataError:
            logger.warning(f"No price data for {country_code} ({start} to {end})")
            return None, None

    # ========================================================================
    # LOAD FORECAST DATA
    # ========================================================================

    def query_load_forecast(
        self,
        country_code: str,
        start: datetime,
        end: datetime,
        process_type: str = 'A01'
    ) -> Optional[pd.DataFrame]:
        """
        Query load forecast data (day-ahead or week-ahead)

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)
            process_type: 'A01' for day-ahead, 'A31' for week-ahead

        Returns:
            DataFrame with columns: timestamp_utc, forecast_value_mw
            None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Query load forecast data
            df = self._make_request(
                self.client.query_load_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                process_type=process_type
            )

            # Handle Series or DataFrame response
            if isinstance(df, pd.Series):
                df = df.to_frame(name='forecast_value_mw')
                df.index.name = 'timestamp_utc'
                df = df.reset_index()
            elif isinstance(df, pd.DataFrame):
                # If DataFrame, take first column
                df = df.iloc[:, [0]].copy()
                df.columns = ['forecast_value_mw']
                df.index.name = 'timestamp_utc'
                df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data (use same limits as load)
            df = utils.remove_outliers(df, 'forecast_value_mw', 'load')

            forecast_type = 'day-ahead' if process_type == 'A01' else 'week-ahead'
            logger.info(f"Retrieved {len(df)} {forecast_type} forecast records for {country_code}")
            return df

        except ENTSOENoDataError:
            logger.warning(f"No load forecast data for {country_code} ({start} to {end}, process_type={process_type})")
            return None

    def query_load_forecast_with_metadata(
        self,
        country_code: str,
        start: datetime,
        end: datetime,
        process_type: str = 'A01'
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Query load forecast data with publication timestamp metadata

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)
            process_type: 'A01' for day-ahead, 'A31' for week-ahead

        Returns:
            Tuple of (dataframe, publication_timestamp)
            - DataFrame with columns: timestamp_utc, forecast_value_mw
            - For week-ahead (A31): Also includes forecast_min_mw, forecast_max_mw
            - Publication timestamp when ENTSO-E created the data
            Both are None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Fetch raw XML first to extract publication timestamp
            raw_xml = self._make_request(
                self.raw_client.query_load_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                process_type=process_type
            )

            # Extract publication timestamp from XML
            publication_time = self._extract_publication_timestamp(raw_xml)

            # For week-ahead (A31), try to parse min/max from XML
            if process_type == 'A31':
                df = self._parse_week_ahead_min_max(raw_xml)
                if df is not None and not df.empty:
                    # Ensure timestamps are UTC
                    df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')
                    logger.info(f"Retrieved {len(df)} week-ahead forecast records with min/max for {country_code} (published: {publication_time})")
                    return df, publication_time

            # Day-ahead (A01) or fallback: use pandas client
            df = self._make_request(
                self.client.query_load_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                process_type=process_type
            )

            # Handle Series or DataFrame response
            if isinstance(df, pd.Series):
                df = df.to_frame(name='forecast_value_mw')
                df.index.name = 'timestamp_utc'
                df = df.reset_index()
            elif isinstance(df, pd.DataFrame):
                # If DataFrame, take first column
                df = df.iloc[:, [0]].copy()
                df.columns = ['forecast_value_mw']
                df.index.name = 'timestamp_utc'
                df = df.reset_index()

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Validate data (use same limits as load)
            df = utils.remove_outliers(df, 'forecast_value_mw', 'load')

            forecast_type = 'day-ahead' if process_type == 'A01' else 'week-ahead'
            logger.info(f"Retrieved {len(df)} {forecast_type} forecast records for {country_code} (published: {publication_time})")
            return df, publication_time

        except ENTSOENoDataError:
            logger.warning(f"No load forecast data for {country_code} ({start} to {end}, process_type={process_type})")
            return None, None

    # ========================================================================
    # WIND & SOLAR GENERATION FORECAST DATA
    # ========================================================================

    def query_wind_solar_forecast(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Query day-ahead wind and solar generation forecast

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            DataFrame with columns: timestamp_utc, solar_mw, wind_onshore_mw, wind_offshore_mw
            None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Query wind and solar forecast data
            df = self._make_request(
                self.client.query_wind_and_solar_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts
            )

            if df is None or df.empty:
                raise ENTSOENoDataError("No wind & solar forecast data returned")

            # Reset index to make timestamp a column
            df = df.reset_index()

            # Ensure timestamp column name
            if 'index' in df.columns:
                df.rename(columns={'index': 'timestamp_utc'}, inplace=True)

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Map columns to our naming convention
            df_result = self._map_wind_solar_forecast_columns(df)

            logger.info(f"Retrieved {len(df_result)} wind & solar forecast records for {country_code}")
            return df_result

        except ENTSOENoDataError:
            logger.warning(f"No wind & solar forecast data for {country_code} ({start} to {end})")
            return None

        except Exception as e:
            logger.error(f"Error querying wind & solar forecast data for {country_code}: {utils.format_error(e)}")
            return None

    def query_wind_solar_forecast_with_metadata(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Query day-ahead wind and solar generation forecast with publication timestamp metadata

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            Tuple of (dataframe, publication_timestamp)
            - DataFrame with columns: timestamp_utc, solar_mw, wind_onshore_mw, wind_offshore_mw
            - Publication timestamp when ENTSO-E created the data
            Both are None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Fetch raw XML first to extract publication timestamp
            raw_xml = self._make_request(
                self.raw_client.query_wind_and_solar_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                psr_type=None
            )

            # Extract publication timestamp from XML
            publication_time = self._extract_publication_timestamp(raw_xml)

            # Now get the data using the pandas client (existing logic)
            df = self._make_request(
                self.client.query_wind_and_solar_forecast,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts
            )

            if df is None or df.empty:
                raise ENTSOENoDataError("No wind & solar forecast data returned")

            # Reset index to make timestamp a column
            df = df.reset_index()

            # Ensure timestamp column name
            if 'index' in df.columns:
                df.rename(columns={'index': 'timestamp_utc'}, inplace=True)

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Map columns to our naming convention
            df_result = self._map_wind_solar_forecast_columns(df)

            logger.info(f"Retrieved {len(df_result)} wind & solar forecast records for {country_code} (published: {publication_time})")
            return df_result, publication_time

        except ENTSOENoDataError:
            logger.warning(f"No wind & solar forecast data for {country_code} ({start} to {end})")
            return None, None

        except Exception as e:
            logger.error(f"Error querying wind & solar forecast data for {country_code}: {utils.format_error(e)}")
            return None, None

    def _map_wind_solar_forecast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map ENTSO-E wind & solar forecast columns to our naming convention

        Args:
            df: DataFrame from ENTSO-E with forecast columns

        Returns:
            DataFrame with mapped columns
        """
        # Initialize result DataFrame with timestamp
        result = pd.DataFrame()
        result['timestamp_utc'] = df['timestamp_utc']

        # Initialize all forecast columns with 0
        for col in ['solar_mw', 'wind_onshore_mw', 'wind_offshore_mw']:
            result[col] = 0.0

        # Map columns based on ENTSO-E naming
        column_mapping = {
            'Solar': 'solar_mw',
            'Wind Onshore': 'wind_onshore_mw',
            'Wind Offshore': 'wind_offshore_mw',
        }

        # Map data
        for entsoe_col, our_col in column_mapping.items():
            if entsoe_col in df.columns:
                result[our_col] = df[entsoe_col].fillna(0)

        return result

    # ========================================================================
    # RENEWABLE GENERATION DATA
    # ========================================================================

    def query_generation_per_type(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Query actual generation per production type (renewables)

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            DataFrame with renewable source columns
            None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Query generation data
            df = self._make_request(
                self.client.query_generation,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                psr_type=None  # Get all production types
            )

            if df is None or df.empty:
                raise ENTSOENoDataError("No generation data returned")

            # Handle MultiIndex columns (production type, data type)
            # entsoe-py returns columns like ('Solar', 'Actual Aggregated')
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten MultiIndex: prefer 'Actual Aggregated' over 'Actual Consumption'
                new_columns = {}
                for col in df.columns:
                    prod_type, data_type = col
                    # Skip consumption data (e.g., Hydro Pumped Storage consumption)
                    if 'Consumption' in data_type:
                        continue
                    # Use just the production type as column name
                    if prod_type not in new_columns:
                        new_columns[prod_type] = df[col]
                df = pd.DataFrame(new_columns, index=df.index)

            # Reset index to make timestamp a column
            df = df.reset_index()

            # Ensure timestamp column name
            if 'index' in df.columns:
                df.rename(columns={'index': 'timestamp_utc'}, inplace=True)

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Map ENTSO-E PSR types to our database columns
            df_renewable = self._map_renewable_columns(df)

            logger.info(f"Retrieved {len(df_renewable)} renewable records for {country_code}")
            return df_renewable

        except ENTSOENoDataError:
            logger.warning(f"No renewable data for {country_code} ({start} to {end})")
            return None

        except Exception as e:
            logger.error(f"Error querying renewable data for {country_code}: {utils.format_error(e)}")
            return None

    def query_generation_per_type_with_metadata(
        self,
        country_code: str,
        start: datetime,
        end: datetime
    ) -> Tuple[Optional[pd.DataFrame], Optional[datetime]]:
        """
        Query actual generation per production type with publication timestamp metadata

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)

        Returns:
            Tuple of (dataframe, publication_timestamp)
            - DataFrame with renewable source columns
            - Publication timestamp when ENTSO-E created the data
            Both are None if no data available
        """
        try:
            # Get country ENTSO-E domain
            country = self._get_country_domain(country_code)

            # Convert to pandas Timestamps (required by entsoe-py)
            start_ts = pd.Timestamp(start).tz_convert('UTC') if hasattr(start, 'tzinfo') and start.tzinfo else pd.Timestamp(start, tz='UTC')
            end_ts = pd.Timestamp(end).tz_convert('UTC') if hasattr(end, 'tzinfo') and end.tzinfo else pd.Timestamp(end, tz='UTC')

            # Fetch raw XML first to extract publication timestamp
            raw_xml = self._make_request(
                self.raw_client.query_generation,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                psr_type=None  # Get all production types
            )

            # Extract publication timestamp from XML
            publication_time = self._extract_publication_timestamp(raw_xml)

            # Now get the data using the pandas client (existing logic)
            df = self._make_request(
                self.client.query_generation,
                country['entsoe_domain'],
                start=start_ts,
                end=end_ts,
                psr_type=None  # Get all production types
            )

            if df is None or df.empty:
                raise ENTSOENoDataError("No generation data returned")

            # Handle MultiIndex columns (production type, data type)
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten MultiIndex: prefer 'Actual Aggregated' over 'Actual Consumption'
                new_columns = {}
                for col in df.columns:
                    prod_type, data_type = col
                    # Skip consumption data
                    if 'Consumption' in data_type:
                        continue
                    # Use just the production type as column name
                    if prod_type not in new_columns:
                        new_columns[prod_type] = df[col]
                df = pd.DataFrame(new_columns, index=df.index)

            # Reset index to make timestamp a column
            df = df.reset_index()

            # Ensure timestamp column name
            if 'index' in df.columns:
                df.rename(columns={'index': 'timestamp_utc'}, inplace=True)

            # Ensure timestamps are UTC
            df['timestamp_utc'] = utils.ensure_timezone_aware(df['timestamp_utc'], 'UTC')

            # Map ENTSO-E PSR types to our database columns
            df_renewable = self._map_renewable_columns(df)

            logger.info(f"Retrieved {len(df_renewable)} renewable records for {country_code} (published: {publication_time})")
            return df_renewable, publication_time

        except ENTSOENoDataError:
            logger.warning(f"No renewable data for {country_code} ({start} to {end})")
            return None, None

        except Exception as e:
            logger.error(f"Error querying renewable data for {country_code}: {utils.format_error(e)}")
            return None, None

    def _map_renewable_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map ENTSO-E generation columns to our renewable energy columns

        Args:
            df: DataFrame from ENTSO-E with generation columns

        Returns:
            DataFrame with mapped renewable columns
        """
        # Initialize result DataFrame with timestamp
        result = pd.DataFrame()
        result['timestamp_utc'] = df['timestamp_utc']

        # Initialize all renewable columns with 0
        renewable_cols = config.get_renewable_columns()
        for col in renewable_cols:
            result[col] = 0.0

        # Map columns based on ENTSO-E naming
        # ENTSO-E columns typically have names like "Solar", "Wind Onshore", etc.
        column_mapping = {
            # ENTSO-E column name patterns -> our column names
            'Solar': 'solar_mw',
            'Wind Onshore': 'wind_onshore_mw',
            'Wind Offshore': 'wind_offshore_mw',
            'Hydro Run-of-river and poundage': 'hydro_run_mw',
            'Hydro Water Reservoir': 'hydro_reservoir_mw',
            'Hydro Pumped Storage': 'hydro_reservoir_mw',  # Combine with reservoir
            'Biomass': 'biomass_mw',
            'Geothermal': 'geothermal_mw',
            'Energy storage': 'other_renewable_mw',  # B25 - battery storage
            'Other renewable': 'other_renewable_mw',  # B15
            'Marine': 'other_renewable_mw',  # B13 - tidal/wave
        }

        # Map data
        for entsoe_col, our_col in column_mapping.items():
            if entsoe_col in df.columns:
                # If column already exists, add to it (for Hydro Pumped Storage case)
                if our_col in result.columns and result[our_col].sum() > 0:
                    result[our_col] += df[entsoe_col].fillna(0)
                else:
                    result[our_col] = df[entsoe_col].fillna(0)

        # Group by timestamp and sum (in case of duplicate timestamps with different types)
        result = result.groupby('timestamp_utc').sum().reset_index()

        return result

    # ========================================================================
    # CROSS-BORDER FLOWS & NET POSITION
    # ========================================================================

    def query_crossborder_all(
        self,
        country_code: str,
        start: datetime,
        end: datetime,
        export: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Query all cross-border physical flows for a country.

        Uses entsoe-py's query_physical_crossborder_allborders which
        internally queries each neighbor and returns a wide DataFrame.

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)
            export: True for exports from country, False for imports

        Returns:
            DataFrame with columns = neighbor zone names + 'sum',
            index = timestamps. None if no data.
        """
        try:
            start_ts = pd.Timestamp(start, tz="UTC")
            end_ts = pd.Timestamp(end, tz="UTC")

            self._rate_limit()
            df = self.client.query_physical_crossborder_allborders(
                country_code, start=start_ts, end=end_ts, export=export
            )

            if df is None or df.empty:
                logger.warning(f"No crossborder flow data for {country_code}")
                return None

            logger.info(
                f"Retrieved crossborder flows for {country_code} "
                f"({'export' if export else 'import'}): "
                f"{len(df)} rows, {len(df.columns)} borders"
            )
            return df

        except Exception as e:
            if "No matching data" in str(e) or "NoMatchingDataError" in type(e).__name__:
                logger.warning(f"No crossborder data for {country_code}: {e}")
                return None
            raise

    def query_net_position_data(
        self,
        country_code: str,
        start: datetime,
        end: datetime,
        dayahead: bool = True,
    ) -> Optional[pd.Series]:
        """
        Query realized net position for a country.

        Args:
            country_code: ISO 2-letter country code
            start: Start datetime (UTC)
            end: End datetime (UTC)
            dayahead: True for day-ahead, False for intraday

        Returns:
            pd.Series with timestamp index and MW values. None if no data.
        """
        try:
            start_ts = pd.Timestamp(start, tz="UTC")
            end_ts = pd.Timestamp(end, tz="UTC")

            self._rate_limit()
            series = self.client.query_net_position(
                country_code, start=start_ts, end=end_ts, dayahead=dayahead
            )

            if series is None or series.empty:
                logger.warning(f"No net position data for {country_code}")
                return None

            logger.info(f"Retrieved {len(series)} net position records for {country_code}")
            return series

        except Exception as e:
            if "No matching data" in str(e) or "NoMatchingDataError" in type(e).__name__:
                logger.warning(f"No net position data for {country_code}: {e}")
                return None
            raise

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    # Bidding zone mappings for price data
    # Some countries use different bidding zones for price vs load data
    PRICE_BIDDING_ZONES = {
        'DE': 'DE_LU',    # Germany uses DE-LU bidding zone for prices
        'LU': 'DE_LU',    # Luxembourg uses DE-LU bidding zone for prices
        'AT': 'AT',       # Austria has its own zone
        'IT': 'IT_NORD',  # Italy uses IT_NORD (Northern Italy) for national price reference
    }

    # Countries with multiple bidding zones that need aggregation
    # These countries don't support country-level price queries, must query each zone
    MULTI_ZONE_COUNTRIES = {
        'NO': ['NO_1', 'NO_2', 'NO_3', 'NO_4', 'NO_5'],  # Norway: 5 price zones
        'SE': ['SE_1', 'SE_2', 'SE_3', 'SE_4'],          # Sweden: 4 price zones
        'DK': ['DK_1', 'DK_2'],                           # Denmark: 2 price zones (West/East)
    }

    def _get_price_country_code(self, country_code: str) -> str:
        """
        Get the correct country/bidding zone code for price queries.
        Some countries use different bidding zones for price data.
        """
        return self.PRICE_BIDDING_ZONES.get(country_code, country_code)

    def _is_multi_zone_country(self, country_code: str) -> bool:
        """Check if a country has multiple bidding zones for prices."""
        return country_code in self.MULTI_ZONE_COUNTRIES

    def _get_multi_zone_prices(
        self,
        country_code: str,
        start: 'pd.Timestamp',
        end: 'pd.Timestamp'
    ) -> Optional[pd.DataFrame]:
        """
        Fetch and aggregate prices from multiple bidding zones.
        Returns the average price across all zones for each timestamp.

        Args:
            country_code: ISO 2-letter country code (NO, SE, DK)
            start: Start timestamp
            end: End timestamp

        Returns:
            DataFrame with columns: timestamp_utc, price_eur_mwh (average across zones)
            None if no data available from any zone
        """
        zones = self.MULTI_ZONE_COUNTRIES.get(country_code, [])
        if not zones:
            return None

        all_zone_data = []

        for zone in zones:
            try:
                series = self._make_request(
                    self.client.query_day_ahead_prices,
                    zone,
                    start=start,
                    end=end
                )

                if series is not None and not series.empty:
                    df = series.to_frame(name=zone)
                    all_zone_data.append(df)
                    logger.debug(f"Retrieved {len(df)} price records for zone {zone}")

            except ENTSOENoDataError:
                logger.warning(f"No price data for zone {zone}")
            except Exception as e:
                logger.warning(f"Error fetching price data for zone {zone}: {e}")

        if not all_zone_data:
            logger.warning(f"No price data available from any zone for {country_code}")
            return None

        # Combine all zone data
        combined = pd.concat(all_zone_data, axis=1)

        # Calculate average across all zones for each timestamp
        combined['price_eur_mwh'] = combined.mean(axis=1)

        # Reset index to make timestamp a column
        result = combined[['price_eur_mwh']].reset_index()
        result.columns = ['timestamp_utc', 'price_eur_mwh']

        logger.info(f"Retrieved {len(result)} aggregated price records for {country_code} from {len(all_zone_data)} zones")
        return result

    def _get_country_domain(self, country_code: str) -> dict:
        """
        Get country ENTSO-E domain from database

        Args:
            country_code: ISO 2-letter country code

        Returns:
            Country dictionary with entsoe_domain

        Raises:
            ValueError if country not found
        """
        from . import db  # Import here to avoid circular dependency

        country = db.get_country_by_code(country_code)
        if not country:
            raise ValueError(f"Country not found or has no ENTSO-E domain: {country_code}")

        return country


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    # Test ENTSO-E client
    print("Testing ENTSO-E client...")

    # Setup logging
    utils.setup_logging()

    # Initialize client
    client = ENTSOEClient()

    # Test date range (single day)
    start = pytz.UTC.localize(datetime(2024, 12, 20))
    end = pytz.UTC.localize(datetime(2024, 12, 21))

    # Test load data
    print(f"\n--- Testing Load Data (Germany, {start.date()}) ---")
    load_df = client.query_load('DE', start, end)
    if load_df is not None:
        print(f"Retrieved {len(load_df)} records")
        print(load_df.head())
    else:
        print("No data returned")

    # Test price data
    print(f"\n--- Testing Price Data (Germany, {start.date()}) ---")
    price_df = client.query_day_ahead_prices('DE', start, end)
    if price_df is not None:
        print(f"Retrieved {len(price_df)} records")
        print(price_df.head())
    else:
        print("No data returned")

    # Test renewable data
    print(f"\n--- Testing Renewable Data (Germany, {start.date()}) ---")
    renewable_df = client.query_generation_per_type('DE', start, end)
    if renewable_df is not None:
        print(f"Retrieved {len(renewable_df)} records")
        print(renewable_df.head())
    else:
        print("No data returned")

    print("\n[OK] ENTSO-E client test complete!")
