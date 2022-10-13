from typing import Tuple, List, Dict, Union

from big_query_handler.query_handler import BigQueryManager
from celery_app.cache_manager import CacheManager


class CrimesDataManager:
    """A class that fetch Chicago crimes data from Google BigQuery dataset"""

    @staticmethod
    def get_crimes_primary_type() -> Tuple[str]:
        """Get crimes distinct primary types.
        At first, it tries to get data from cache, if cache
        is empty, it will query data from Google BigQuery dataset

        Returns:
            A tuple containing distinct strings of primary types

        Raises:
            BigQueryManager.QueryTimeoutError
            BigQueryManager.GoogleCloudQueryError
        """

        # getting crimes primary types from cache
        crimes_primary_types = CacheManager.get_crimes_primary_types()
        if crimes_primary_types is None:
            # there is no crimes primary types cached, so let's get them from dataset
            try:
                crimes_primary_types = BigQueryManager().query_crimes_primary_types()
            except BigQueryManager.QueryTimeoutError:
                raise BigQueryManager.QueryTimeoutError
            except BigQueryManager.GoogleCloudQueryError:
                raise BigQueryManager.GoogleCloudQueryError
            except Exception as ex:
                raise ex

            # cache was empty, so cache fetched data
            CacheManager.set_crimes_primary_types(crimes_primary_types)
        return crimes_primary_types

    @staticmethod
    def get_crimes_by_primary_type(primary_type: str) -> List[Dict[str, Union[float, str]]]:
        """Get crimes of primary type.
        At first, it tries to get data from cache, if cache
        is empty, it will query data from Google BigQuery dataset

        Args:
            primary_type (str): A string that indicates primary type

        Returns:
             A list of crimes that contains crime location and date in a dict.

        Raises:
            BigQueryManager.QueryTimeoutError
            BigQueryManager.GoogleCloudQueryError
        """

        # getting crimes data from cache
        crimes_by_primary_type = CacheManager.get_crimes_by_primary_type(primary_type)
        if crimes_by_primary_type is None:
            # there is no cached crimes of primary types, so fetching data from dataset
            try:
                crimes_by_primary_type = BigQueryManager().query_crimes_by_primary_type(primary_type)
            except BigQueryManager.QueryTimeoutError:
                raise BigQueryManager.QueryTimeoutError
            except BigQueryManager.GoogleCloudQueryError:
                raise BigQueryManager.GoogleCloudQueryError
            except Exception as ex:
                raise ex
            # cache fetched data for crimes of primary type
            CacheManager.set_crimes_filtered_by_primary_type(primary_type, crimes_by_primary_type)
        return crimes_by_primary_type
