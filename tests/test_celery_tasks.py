import unittest

import pytest

from celery_app.tasks import get_crimes_by_primary_type_from_bigquery_and_cache


# there will be 2 DeprecationWarning while we are using pytest
# it is because of kombu and celery and python 3.10
# there is a pre-release for kombu that does not produce this warning


class TestCeleryTasks(unittest.TestCase):
    @pytest.mark.celery(result_backend='redis://localhost:6379')
    def test_getting_crimes_of_primary_type_and_cache(self):
        default_primary_type = 'HOMICIDE'
        task = get_crimes_by_primary_type_from_bigquery_and_cache.apply(args=(default_primary_type,))
        task_result = task.get()
        self.assertTrue(task_result, 'Task failed')
