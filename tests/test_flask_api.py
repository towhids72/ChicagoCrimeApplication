import datetime
import unittest

from api.app import application


class TestFlaskAPIEndpoints(unittest.TestCase):
    def test_crimes_primary_types(self):
        response = application.test_client().get('/api/crimes/primary_types')
        self.assertTrue(response.status_code == 200)
        primary_types = response.json['data']
        self.assertIsInstance(primary_types, list)
        self.assertTrue(any([isinstance(item, str) for item in primary_types]))

    def test_crimes_by_primary_type(self):
        default_primary_type = 'HOMICIDE'
        response = application.test_client().get('api/crimes/', query_string={"primary_type": default_primary_type})
        self.assertTrue(response.status_code == 200)
        crimes = response.json['data']
        self.assertIsInstance(crimes, list)
        self.assertTrue(any([isinstance(crime['lat'], float) for crime in crimes]))
        self.assertTrue(([isinstance(crime['lon'], float) for crime in crimes]))
        self.assertTrue(any([datetime.datetime.strptime(crime['date'], '%Y-%m-%d') for crime in crimes]))
