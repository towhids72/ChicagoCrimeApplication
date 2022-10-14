import logging
import os
import pickle
from typing import Optional, Tuple, List, Dict, Union

import redis
from dotenv import load_dotenv

from utilities.log_utils import LogUtils

load_dotenv()
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = os.environ.get('REDIS_PORT', 6379)
redis_db = os.environ.get('REDIS_DB', 0)

logger = LogUtils.get_logger(logger_name='cache_manager', level=logging.ERROR)


class RedisUtils:
    """A class that instantiate a redis object to communicate with redis"""

    __redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db
    )

    @staticmethod
    def get_redis_client() -> redis.StrictRedis:
        """Returns instantiated redis objects

        Returns:
            An object to communicate(set and get) with redis
        """
        return RedisUtils.__redis_client


class CacheManager:
    """A class that simplify setting and getting data in/from redis"""

    __crimes_primary_type_key = 'CrimesPrimaryType'

    @staticmethod
    def crimes_by_primary_type_key_generator(primary_type: str) -> str:
        """Generates a unique key based on given primary type

        Args:
            primary_type (str): A string of crime primary type

        Returns:
            A string that is unique to crime primary type
        """

        # remove spaces and replace dashes with underline to be a meaningful key
        primary_type = primary_type.replace(' ', '').replace('-', '_')
        return f'CrimesByType_{primary_type}'

    @staticmethod
    def set_crimes_primary_types(value: Tuple[str]) -> bool:
        """Pickles and sets primary types data to redis

        Args:
             value: A tuple of primary types

        Returns:
            A boolean value that shows data cached successfully or not
        """

        try:
            redis_client = RedisUtils.get_redis_client()
            redis_client.set(name=CacheManager.__crimes_primary_type_key, value=pickle.dumps(value))
            return True
        except Exception:
            logger.exception('Can not save primary types data to cache, maybe redis is not ready')
            return False

    @staticmethod
    def get_crimes_primary_types() -> Optional[Tuple[str]]:
        """Gets and returns cached crimes primary types, and
        returns None if cache is empty

        Returns:
            A tuple containing distinct strings of primary types or None
        """

        try:
            redis_client = RedisUtils.get_redis_client()
            crimes_primary_types = redis_client.get(name=CacheManager.__crimes_primary_type_key)
            if crimes_primary_types:
                return pickle.loads(crimes_primary_types)
        except Exception:
            logger.exception('Can not get primary types data from cache, maybe redis is not ready')
        return

    @staticmethod
    def set_crimes_filtered_by_primary_type(primary_type: str, value: List[Dict[str, Union[float, str]]]) -> bool:
        """Pickles and sets crimes data to redis

        Args:
            value: A list of crimes that contains crime location and date in a dict.
            primary_type (str): A string of crime primary type

        Returns:
            A boolean value that shows data cached successfully or not
        """

        # generate a key to cache crimes data, we will use this key to fetch cached data
        key = CacheManager.crimes_by_primary_type_key_generator(primary_type)
        try:
            redis_client = RedisUtils.get_redis_client()
            redis_client.set(name=key, value=pickle.dumps(value))
            return True
        except Exception:
            logger.exception('Can not save crimes data to cache, maybe redis is not ready')
            return False

    @staticmethod
    def get_crimes_by_primary_type(primary_type: str) -> Optional[List[Dict[str, Union[float, str]]]]:
        """Gets and returns cached crimes data of given primary type,
        and returns None if cache is empty.

        Args:
              primary_type (str): A string of crime primary type.

        Returns:
              A list of crimes that contains crime location and date in a dict or None.
        """

        # we used this function to generate a key when we were caching crime data, so now we use it to fetch data
        key = CacheManager.crimes_by_primary_type_key_generator(primary_type)
        try:
            redis_client = RedisUtils.get_redis_client()
            crimes_by_primary_type = redis_client.get(name=key)
            if crimes_by_primary_type:
                return pickle.loads(crimes_by_primary_type)
        except Exception:
            logger.exception('Can not get crimes data from cache, maybe redis is not ready')
        return
