import logging
import os
import pickle

import redis
from dotenv import load_dotenv

from utilities.log_utils import LogUtils

load_dotenv()
redis_host = os.environ.get('REDIS_HOST', 'redis')
redis_port = os.environ.get('REDIS_PORT', 6379)
redis_db = os.environ.get('REDIS_DB', 0)

logger = LogUtils.get_logger(logger_name='cache_manager', level=logging.ERROR)


class RedisUtils:
    __redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db
    )

    @staticmethod
    def get_redis_client():
        return RedisUtils.__redis_client


class CacheManager:
    __crimes_primary_type_key = 'CrimesPrimaryType'

    @staticmethod
    def crimes_by_primary_type_key_generator(primary_type: str) -> str:
        primary_type = primary_type.replace(' ', '').replace('-', '_')
        return f'CrimesByType_{primary_type}'

    @staticmethod
    def set_crimes_primary_types(value) -> bool:
        try:
            redis_client = RedisUtils.get_redis_client()
            redis_client.set(name=CacheManager.__crimes_primary_type_key, value=pickle.dumps(value))
            return True
        except Exception:
            logger.exception('Can not save primary types data to cache, maybe redis is not ready')
            return False

    @staticmethod
    def get_crimes_primary_types():
        try:
            redis_client = RedisUtils.get_redis_client()
            crimes_primary_types = redis_client.get(name=CacheManager.__crimes_primary_type_key)
            if crimes_primary_types:
                return pickle.loads(crimes_primary_types)
        except Exception:
            logger.exception('Can not get primary types data from cache, maybe redis is not ready')
        return

    @staticmethod
    def set_crimes_filtered_by_primary_type(primary_type: str, value) -> bool:
        key = CacheManager.crimes_by_primary_type_key_generator(primary_type)
        try:
            redis_client = RedisUtils.get_redis_client()
            redis_client.set(name=key, value=pickle.dumps(value))
            return True
        except Exception:
            logger.exception('Can not save crimes data to cache, maybe redis is not ready')
            return False

    @staticmethod
    def get_crimes_by_primary_type(primary_type: str):
        key = CacheManager.crimes_by_primary_type_key_generator(primary_type)
        try:
            redis_client = RedisUtils.get_redis_client()
            crimes_by_primary_type = redis_client.get(name=key)
            if crimes_by_primary_type:
                return pickle.loads(crimes_by_primary_type)
        except Exception:
            logger.exception('Can not get crimes data from cache, maybe redis is not ready')
        return
