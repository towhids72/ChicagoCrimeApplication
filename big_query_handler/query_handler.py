import os
from typing import List, Dict

from google.cloud import bigquery

proxy = 'http://127.0.0.1:18080'
os.environ['http_proxy'] = proxy
os.environ['HTTP_PROXY'] = proxy
os.environ['https_proxy'] = proxy
os.environ['HTTPS_PROXY'] = proxy


class BigQueryManager:
    def __init__(self):
        self.client = bigquery.Client()
        self.query_timeout = 60

    def query_crimes_by_primary_type(self, primary_type: str):
        query_expression = (
            f'SELECT latitude, longitude '
            f'FROM `bigquery-public-data.chicago_crime.crime` '
            f'WHERE primary_type="{primary_type}" '
            f'GROUP BY latitude, longitude LIMIT 2000'
        )

        query_job = self.client.query(query=query_expression, timeout=self.query_timeout)
        query_response = query_job.result()

        fetched_crimes_locations: List[Dict[str, float]] = [
            {
                'lat': item[0],
                'lon': item[1]
            } for item in query_response if item[0] and item[1]
        ]
        return fetched_crimes_locations

    def query_crimes_primary_types(self):
        query_expression = (
            f'SELECT DISTINCT(primary_type) '
            f'FROM `bigquery-public-data.chicago_crime.crime`'
        )

        query_job = self.client.query(query=query_expression, timeout=60)
        query_response = query_job.result()

        return list(set([
            item[0].replace(' - ', '-') for item in query_response
        ]))


if __name__ == '__main__':
    BigQueryManager().query_crimes_primary_types()
