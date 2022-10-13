import logging
from http import HTTPStatus

from flask import Blueprint, request

from api.api_response import APIResponse
from api.services import CrimesDataManager
from big_query_handler.query_handler import BigQueryManager
from utilities.log_utils import LogUtils

logger = LogUtils.get_logger(logger_name='flask_api', level=logging.ERROR)

# create a blueprint for crimes endpoints and set url prefix for this routes
chicago_crimes_blueprint = Blueprint('chicago_crimes_routes', __name__, url_prefix='/api/crimes')


# noinspection PyTypeChecker
@chicago_crimes_blueprint.route('/primary_types', methods=['GET'])
def get_chicago_crimes_primary_types() -> APIResponse:
    """Returns all distinct crimes primary types

    Responses part can be used by auto doc generators like `swagger`

    Returns:
        An APIResponse which contains JSON data and proper HTTP status
    Responses:
        * 200: all distinct crimes primary types.
        * 408: request timed out from data provider.
        * 500: can not connect to data provider.
        * 503: service currently is unavailable.
    """
    try:
        crimes_primary_types = CrimesDataManager.get_crimes_primary_type()
        return APIResponse.ok_response(data=crimes_primary_types)
    except BigQueryManager.QueryTimeoutError:
        logger.error('BigQuery timeout error')
        return APIResponse.error_response(HTTPStatus.REQUEST_TIMEOUT)
    except BigQueryManager.GoogleCloudQueryError:
        logger.error('BigQuery does not provide data, maybe credential is missing!')
        return APIResponse.error_response(HTTPStatus.INTERNAL_SERVER_ERROR)
    except Exception:
        # we should capture this kind of exceptions somewhere like Slack ot Telegram to get notify
        logger.exception('Error while getting crimes primary types')
        return APIResponse.error_response(HTTPStatus.SERVICE_UNAVAILABLE)


# noinspection PyTypeChecker
@chicago_crimes_blueprint.route('/', methods=['GET'])
def get_chicago_crimes_by_primary_type():
    """Returns crimes of primary type that is sent in query params

    Responses part can be used by auto doc generators like `swagger`

    Returns:
        An APIResponse which contains JSON data and proper HTTP status

    Responses:
        * 200: A list of latitude, longitude, and the date of the crime.
        * 408: request timed out from data provider.
        * 500: can not connect to data provider.
        * 503: service currently is unavailable.
    """
    primary_type = request.args.get('primary_type', None, str)
    try:
        # here we can set a default primary type when it is not sent by the request, but while we are getting it
        # from Streamlit dashboard it's better to notify the error, maybe Streamlit has gone wrong!
        if primary_type is None:
            return APIResponse.error_response(HTTPStatus.BAD_REQUEST)
        crimes_by_primary_type = CrimesDataManager.get_crimes_by_primary_type(primary_type)
        return APIResponse.ok_response(data=crimes_by_primary_type)
    except BigQueryManager.QueryTimeoutError:
        logger.error('BigQuery timeout error')
        return APIResponse.error_response(HTTPStatus.REQUEST_TIMEOUT)
    except BigQueryManager.GoogleCloudQueryError:
        logger.error('BigQuery does not provide data, maybe credential is missing!')
        return APIResponse.error_response(HTTPStatus.INTERNAL_SERVER_ERROR)
    except Exception:
        # we should capture this kind of exceptions somewhere like Slack ot Telegram to get notify
        logger.exception(f'Error while getting crimes of primary type')
        return APIResponse.error_response(HTTPStatus.SERVICE_UNAVAILABLE)
