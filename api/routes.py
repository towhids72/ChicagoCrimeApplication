import json

from flask import Blueprint, request

from api.api_response import APIResponse
from big_query_handler.query_handler import BigQueryManager
from celery_app.cache_manager import CacheManager

chicago_crimes_blueprint = Blueprint('chicago_crimes_routes', __name__, url_prefix='/api/crimes')


@chicago_crimes_blueprint.route('/primary_types', methods=['GET'])
def get_chicago_crimes_primary_types():
    try:
        crimes_primary_types = CacheManager.get_crimes_primary_types()
        if crimes_primary_types is None:
            crimes_primary_types = BigQueryManager().query_crimes_primary_types()
            CacheManager.set_crimes_primary_types(crimes_primary_types)
        return APIResponse.ok_response(data=crimes_primary_types)
    except BigQueryManager.QueryTimeoutError:
        return APIResponse.error_response(message='Timeout error', code=408)
    except BigQueryManager.GoogleCloudQueryError:
        return APIResponse.error_response(message='Data fetch error', code=500)
    except Exception:
        # we should capture this kind of exceptions somewhere like Slack ot Telegram to get notify
        return APIResponse.error_response(message='Unexpected error', code=503)


@chicago_crimes_blueprint.route('/', methods=['POST'])
def get_chicago_crimes_by_primary_type():
    try:
        json_body = json.loads(request.data)
        primary_type = json_body.get('primary_type', None)
        if primary_type is None:
            return APIResponse.error_response(message='Primary type can not be null', code=400)
        crimes_by_primary_type = CacheManager.get_crimes_by_primary_type(primary_type)
        if crimes_by_primary_type is None:
            crimes_by_primary_type = BigQueryManager().query_crimes_by_primary_type(primary_type)
            CacheManager.set_crimes_filtered_by_primary_type(primary_type, crimes_by_primary_type)
        return APIResponse.ok_response(data=crimes_by_primary_type)
    except BigQueryManager.QueryTimeoutError:
        return APIResponse.error_response(message='Timeout error', code=408)
    except BigQueryManager.GoogleCloudQueryError:
        return APIResponse.error_response(message='Data fetch error', code=500)
    except Exception:
        # we should capture this kind of exceptions somewhere like Slack ot Telegram to get notify
        return APIResponse.error_response(message='Unexpected error', code=503)
