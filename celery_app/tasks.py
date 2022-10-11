import logging
import os
import time
from typing import Tuple

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready
from dotenv import load_dotenv

from big_query_handler.query_handler import BigQueryManager
from celery_app.cache_manager import CacheManager

load_dotenv()
logging.basicConfig(level=logging.INFO, format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s')
logger = logging.getLogger("chicago_crime")
logger.setLevel(logging.INFO)

celery_broker = os.environ.get("CELERY_BROKER_URL")
celery_backend = os.environ.get("CELERY_RESULT_BACKEND")

celery = Celery(
    main='chicago_crimes', broker=celery_broker, backend=celery_backend,
    include=[
        'celery_app.tasks'
    ]
)


@celery.task(name="get_and_update_crimes_primary_types_in_cache")
def get_and_update_crimes_primary_types_in_cache():
    logger.info('Getting crimes primary types from BigQuery...')
    try:
        crimes_primary_types = BigQueryManager().query_crimes_primary_types()
        CacheManager.set_crimes_primary_types(crimes_primary_types)
        return True
    except Exception as ex:
        # we can use an alerting module to capture this errors
        logger.error(ex)


@celery.task(name='get_crimes_by_primary_type_from_bigquery_and_cache')
def get_crimes_by_primary_type_from_bigquery_and_cache(primary_type: str):
    logger.info(f'Getting and caching {primary_type} crimes data...')
    try:
        crimes_by_primary_type = BigQueryManager().query_crimes_by_primary_type(primary_type)
        CacheManager.set_crimes_filtered_by_primary_type(primary_type, crimes_by_primary_type)
        return True
    except Exception as ex:
        # we can use an alerting module to capture this errors
        logger.error(ex)
        raise ex


@celery.task(name='get_and_update_crimes_by_primary_type')
def get_and_update_crimes_by_primary_type(primary_types: Tuple[str] = ()):
    logger.info(f'Preparing to cache {str(primary_types) if primary_types else "All"} crimes data...')
    if not primary_types:
        primary_types = CacheManager.get_crimes_primary_types()
        if primary_types is None:
            # if primary types cache is empty, so we should query distinct primary types from BigQuery
            primary_types = BigQueryManager().query_crimes_primary_types()
            CacheManager.set_crimes_primary_types(primary_types)

    for primary in primary_types:
        get_crimes_by_primary_type_from_bigquery_and_cache.apply_async(queue='crimes', args=(primary,))

    return True


# noinspection PyUnusedLocal
@worker_ready.connect
def at_start(sender, **kwargs):
    if 'crimes' in sender.app.amqp.queues:
        logger.info('Maybe it is the first time that I am running! so lets cache some data at first')
        # we put process to sleep until everything runs properly, after that we complete "at start" tasks
        time.sleep(10)
        with sender.app.connection() as con:
            sender.app.send_task('get_and_update_crimes_by_primary_type', connection=con, queue='crimes')


# noinspection PyUnusedLocal
@celery.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    logger.info('Set up periodic tasks')

    sender.add_periodic_task(
        crontab(minute="0", hour="0"),
        get_and_update_crimes_primary_types_in_cache.s(),
        name='get_and_update_crimes_primary_types_in_cache',
        queue='crimes'
    )
    sender.add_periodic_task(
        crontab(minute="10", hour="0"),
        get_crimes_by_primary_type_from_bigquery_and_cache.s(),
        name='get_crimes_by_primary_type_from_bigquery_and_cache',
        queue='crimes'
    )
