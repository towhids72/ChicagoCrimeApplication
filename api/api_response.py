from http import HTTPStatus
from typing import Tuple

from flask import jsonify, Response


class APIResponse:
    """A class that is used to unify app's API responses"""

    @staticmethod
    def ok_response(data, http_status: HTTPStatus = HTTPStatus.OK) -> Tuple[Response, HTTPStatus]:
        """Serialize given data and http status as JSON object.

        Args:
            data: any JSON serializable data.
            http_status (HTTPStatus): successful HTTP status code, default HTTPStatus.OK.
        Returns:
            A tuple object that contain JSON data and HTTP status code
        """
        return jsonify(
            {
                "code": http_status,
                "message": http_status.description,
                "data": data
            }
        ), http_status

    @staticmethod
    def error_response(http_status: HTTPStatus) -> Tuple[Response, HTTPStatus]:
        """Serialize an error response with given http status as JSON object.

        Args:
            http_status (HTTPStatus): proper HTTP status code to indicate error type.
        Returns:
            A tuple object that contain JSON data and HTTP status code
        """
        return jsonify(
            {
                "code": http_status,
                "message": http_status.description,
                "data": None
            }
        ), http_status
