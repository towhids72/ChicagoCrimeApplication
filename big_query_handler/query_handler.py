import os
from typing import List, Dict, Tuple, Set

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, GatewayTimeout


class BigQueryManager:
    class GoogleCloudQueryError(Exception):
        pass

    class QueryTimeoutError(Exception):
        pass

    def __init__(self):
        self.client = bigquery.Client()
        self.query_timeout = 60
        # todo remove these files when you send it
        self.proxy = 'http://0.0.0.0:18080'
        os.environ['HTTP_PROXY'] = self.proxy
        os.environ['HTTPS_PROXY'] = self.proxy

    def query_crimes_by_primary_type(self, primary_type: str):
        query_expression = (
            f'SELECT latitude, longitude, DATE(date) AS primary_date '
            f'FROM `bigquery-public-data.chicago_crime.crime` '
            f'WHERE primary_type=@primary_type '
            f'GROUP BY latitude, longitude, primary_date '
            f'ORDER BY primary_date DESC LIMIT 2000'
        )

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("primary_type", "STRING", primary_type)
            ]
        )

        try:
            query_job = self.client.query(query=query_expression, timeout=self.query_timeout, job_config=job_config)
            query_response = query_job.result()
        except GoogleCloudError:
            raise BigQueryManager.GoogleCloudQueryError
        except GatewayTimeout:
            raise BigQueryManager.QueryTimeoutError
        except Exception as ex:
            raise ex

        fetched_crimes_locations: List[Dict[str, float]] = [
            {
                'lat': item[0],
                'lon': item[1],
                'date': item[2].strftime('%Y-%m-%d')
            } for item in query_response if item[0] and item[1]
        ]

        return fetched_crimes_locations

    def query_crimes_primary_types(self) -> Tuple[str]:
        query_expression = (
            'SELECT DISTINCT(primary_type) '
            'FROM `bigquery-public-data.chicago_crime.crime`'
        )

        try:
            query_job = self.client.query(query=query_expression, timeout=60)
            query_response = query_job.result()
        except GoogleCloudError:
            raise BigQueryManager.GoogleCloudQueryError
        except GatewayTimeout:
            raise BigQueryManager.QueryTimeoutError
        except Exception as ex:
            raise ex

        primary_types: Set[str] = set([item[0].replace(' - ', '-') for item in query_response])

        return tuple(primary_types)


if __name__ == '__main__':
    BigQueryManager().query_crimes_by_primary_type('HOMICIDE')
