from http import HTTPStatus

from flask import jsonify


class APIResponse:
    @staticmethod
    def ok_response(data, http_status: HTTPStatus = HTTPStatus.OK):
        return jsonify(
            {
                "code": http_status,
                "message": http_status.description,
                "data": data
            }
        ), http_status

    @staticmethod
    def error_response(http_status: HTTPStatus):
        return jsonify(
            {
                "code": http_status,
                "message": http_status.description,
                "data": []
            }
        ), http_status
