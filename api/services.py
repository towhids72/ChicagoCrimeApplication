from big_query_handler.query_handler import BigQueryManager
from celery_app.cache_manager import CacheManager


class CrimesDataManager:
    @staticmethod
    def get_crimes_primary_type():
        crimes_primary_types = CacheManager.get_crimes_primary_types()
        if crimes_primary_types is None:
            try:
                crimes_primary_types = BigQueryManager().query_crimes_primary_types()
            except BigQueryManager.QueryTimeoutError:
                raise BigQueryManager.QueryTimeoutError
            except BigQueryManager.GoogleCloudQueryError:
                raise BigQueryManager.GoogleCloudQueryError
            except Exception as ex:
                raise ex

            CacheManager.set_crimes_primary_types(crimes_primary_types)
        return crimes_primary_types

    @staticmethod
    def get_crimes_by_primary_type(primary_type: str):
        crimes_by_primary_type = CacheManager.get_crimes_by_primary_type(primary_type)
        if crimes_by_primary_type is None:
            try:
                crimes_by_primary_type = BigQueryManager().query_crimes_by_primary_type(primary_type)
            except BigQueryManager.QueryTimeoutError:
                raise BigQueryManager.QueryTimeoutError
            except BigQueryManager.GoogleCloudQueryError:
                raise BigQueryManager.GoogleCloudQueryError
            except Exception as ex:
                raise ex
            CacheManager.set_crimes_filtered_by_primary_type(primary_type, crimes_by_primary_type)
        return crimes_by_primary_type
