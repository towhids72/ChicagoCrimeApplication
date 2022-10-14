import logging
import os
import time

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready
from dotenv import load_dotenv

from big_query.crimes import BigQueryManager
from celery_app.cache_manager import CacheManager
from utilities.log_utils import LogUtils

# loading environment variables which are defined in .env file
load_dotenv()

redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', 6379)

# create celery broker and backend from redis host that we retrieved from environment variables
celery_broker = f'redis://{redis_host}:{redis_port}'
celery_backend = f'redis://{redis_host}:{redis_port}'

logger = LogUtils.get_logger(logger_name='celery_tasks', level=logging.INFO)

# instantiate celery objects and configure tasks
celery = Celery(
    main='chicago_crimes', broker=celery_broker, backend=celery_backend,
    include=[
        'celery_app.tasks'
    ]
)


@celery.task(name='get_crimes_by_primary_type_from_bigquery_and_cache')
def get_crimes_by_primary_type_from_bigquery_and_cache(primary_type: str) -> bool:
    """A celery task that fetch crimes data of given primary type and caches it.

    Args:
        primary_type (str): A string that indicates primary type.

    Returns:
        A boolean value that shows task was successful or failed.
    """

    logger.info(f'Getting and caching {primary_type} crimes data...')
    try:
        crimes_by_primary_type = BigQueryManager().query_crimes_by_primary_type(primary_type)
        return CacheManager.set_crimes_filtered_by_primary_type(primary_type, crimes_by_primary_type)
    except BigQueryManager.QueryTimeoutError:
        logger.error('BigQuery timeout error')
    except BigQueryManager.GoogleCloudQueryError:
        logger.error('BigQuery does not provide data, maybe credential is missing!')
    except Exception:
        logger.exception('Error while getting crimes of primary type')
    return False


@celery.task(name='get_and_update_crimes_by_primary_type')
def get_and_update_crimes_by_primary_type() -> bool:
    """This celery task gets all crimes primary types and
    fetches and caches crimes data for them.

    Returns:
        A boolean value that shows task was successful or failed.
    """

    try:
        primary_types = BigQueryManager().query_crimes_primary_types()
        # cache fetched crimes primary types
        CacheManager.set_crimes_primary_types(primary_types)
    except BigQueryManager.QueryTimeoutError:
        logger.error('BigQuery timeout error')
        return False
    except BigQueryManager.GoogleCloudQueryError:
        logger.error('BigQuery does not provide data, maybe credential is missing!')
        return False
    except Exception:
        logger.exception('Error while getting crimes of primary type')
        return False

    logger.info(f'Preparing to cache {str(primary_types)} crimes data...')
    for primary_type in primary_types:
        # creating tasks to fetch and cache crimes data
        get_crimes_by_primary_type_from_bigquery_and_cache.apply_async(queue='crimes', args=(primary_type,))

    return True


# noinspection PyUnusedLocal
@worker_ready.connect
def at_start(sender, **kwargs):
    """this function will be called every time celery worker runs to fetch and cache data"""

    if 'crimes' in sender.app.amqp.queues:
        logger.info('Maybe it is the first time that I am running! so lets cache some data at first')
        # we put process to sleep until everything runs properly, after that we complete "at start" tasks
        time.sleep(10)
        with sender.app.connection() as con:
            sender.app.send_task('get_and_update_crimes_by_primary_type', connection=con, queue='crimes')


# noinspection PyUnusedLocal
@celery.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    """This method is used to set up celery periodic tasks"""

    logger.info('Set up periodic tasks')

    # at midnight this task will run to fetch and cache crimes data
    sender.add_periodic_task(
        crontab(minute="0", hour="0"),
        get_and_update_crimes_by_primary_type.s(),
        name='get_and_update_crimes_primary_types_in_cache',
        queue='crimes'
    )
