from typing import List, Dict, Tuple, Set, Union

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, GatewayTimeout


class BigQueryManager:
    """A manager class to query data from Google BigQuery dataset"""

    class GoogleCloudQueryError(Exception):
        """An Exception class to raise when Google returns error on query"""
        pass

    class QueryTimeoutError(Exception):
        """An Exception class to raise when Google doesn't response in given time"""
        pass

    def __init__(self):
        """Initialize BigQuery client and set timeout for it"""
        self.client = bigquery.Client()
        self.query_timeout = 60

    def query_crimes_by_primary_type(self, primary_type: str) -> List[Dict[str, Union[float, str]]]:
        """Fetches data for crime of given primary type.
        Data is sorted based on crime date and limited to 2000 datapoints.

        Args:
            primary_type (str): Crime primary type

        Returns:
            A list of crimes that contains crime location and date in a dict.

        Raises:
            BigQueryManager.GoogleCloudQueryError
            BigQueryManager.QueryTimeoutError
        """

        # a query expression that selects distinct latitude, longitude, and crime date, which is ordered by crimes
        # date and limited to 2000 datapoints.
        query_expression = (
            f'SELECT latitude, longitude, DATE(date) AS crime_date '
            f'FROM `bigquery-public-data.chicago_crime.crime` '
            f'WHERE primary_type=@primary_type '
            f'GROUP BY latitude, longitude, crime_date '
            f'ORDER BY crime_date DESC LIMIT 2000'
        )

        # because of primary_type variable in query, we use QueryJobConfig to prevent SQL injection
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("primary_type", "STRING", primary_type)
            ]
        )

        try:
            # query the data from BigQuery public dataset
            query_job = self.client.query(query=query_expression, timeout=self.query_timeout, job_config=job_config)
            query_response = query_job.result()
        except GoogleCloudError:
            raise BigQueryManager.GoogleCloudQueryError
        except GatewayTimeout:
            raise BigQueryManager.QueryTimeoutError
        except Exception as ex:
            raise ex

        # create a list of crimes that contains crime latitude, longitude, and crime date
        # there some null datapoints in query result, we skip them when creating the crimes data
        fetched_crimes_locations: List[Dict[str, float]] = [
            {
                'lat': item[0],
                'lon': item[1],
                'date': item[2].strftime('%Y-%m-%d')
            } for item in query_response if item[0] and item[1] and item[2]
        ]

        return fetched_crimes_locations

    def query_crimes_primary_types(self) -> Tuple[str]:
        """Returns a tuple of distinct crimes primary type

        Returns:
             A tuple containing distinct strings of primary types

        Raises:
            BigQueryManager.GoogleCloudQueryError
            BigQueryManager.QueryTimeoutError
        """

        # select distinct crimes type
        query_expression = (
            'SELECT DISTINCT(primary_type) '
            'FROM `bigquery-public-data.chicago_crime.crime`'
        )

        try:
            # query the data from BigQuery public dataset
            query_job = self.client.query(query=query_expression, timeout=60)
            query_response = query_job.result()
        except GoogleCloudError:
            raise BigQueryManager.GoogleCloudQueryError
        except GatewayTimeout:
            raise BigQueryManager.QueryTimeoutError
        except Exception as ex:
            raise ex

        # there is some duplicate values like: "NON - CRIMINAL" and "NON-CRIMINAL"
        # We should omit "NON - CRIMINAL" because there is only a few data points for this type,
        # and also we want unique crimes type, so we clear the text then convert crimes type to a set
        primary_types: Set[str] = set([item[0].replace(' - ', '-') for item in query_response])

        # convert crimes type to tuple because later we want to query crimes based on these types,
        # so they must not be mutable
        return tuple(primary_types)


if __name__ == '__main__':
    BigQueryManager().query_crimes_by_primary_type('HOMICIDE')
