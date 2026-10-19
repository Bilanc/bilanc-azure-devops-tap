import json
import unittest

import requests

import tap_azure_devops
from tap_azure_devops import (
    BadRequestException,
    AuthException,
    NotFoundException,
    raise_for_error,
)


def response(status, body=None, url="https://dev.azure.com/org/Proj/_apis/build/builds"):
    result = requests.Response()
    result.status_code = status
    result.url = url
    result._content = json.dumps(body).encode() if body is not None else b""
    return result


PROJECT_DOES_NOT_EXIST_BODY = {
    "$id": "1",
    "innerException": None,
    "message": (
        "VS800075: The project with id 'vstfs:///Classification/TeamProject/"
        "e61c5d3a-95db-4a0f-a4f4-5504b2c90551' does not exist, or you do not have "
        "permission to access it."
    ),
    "typeName": (
        "Microsoft.TeamFoundation.Core.WebApi.ProjectDoesNotExistException, "
        "Microsoft.TeamFoundation.Core.WebApi"
    ),
    "typeKey": "ProjectDoesNotExistException",
    "errorCode": 0,
    "eventId": 3000,
}


class TestRaiseForError(unittest.TestCase):
    def test_400_project_does_not_exist_is_not_found(self):
        with self.assertRaises(NotFoundException) as ctx:
            raise_for_error(response(400, PROJECT_DOES_NOT_EXIST_BODY), "builds")
        self.assertIn("Project does not exist", str(ctx.exception))
        self.assertIn("/Proj/_apis/build/builds", str(ctx.exception))

    def test_403_project_does_not_exist_is_not_found(self):
        with self.assertRaises(NotFoundException):
            raise_for_error(response(403, PROJECT_DOES_NOT_EXIST_BODY), "pipelines")

    def test_400_vs800075_message_without_type_key_is_not_found(self):
        body = {"message": "VS800075: The project does not exist."}
        with self.assertRaises(NotFoundException):
            raise_for_error(response(400, body), "builds")

    def test_other_400_is_still_bad_request(self):
        body = {"message": "VS400000: bad $top", "typeKey": "ArgumentException"}
        with self.assertRaises(BadRequestException):
            raise_for_error(response(400, body), "builds")

    def test_other_403_is_still_auth_error(self):
        with self.assertRaises(AuthException):
            raise_for_error(response(403, {"message": "TF400813: not authorized"}), "builds")

    def test_400_with_non_json_body_is_bad_request(self):
        result = requests.Response()
        result.status_code = 400
        result.url = "https://dev.azure.com/org/_apis/x"
        result._content = b"<html>bad</html>"
        with self.assertRaises(BadRequestException):
            raise_for_error(result, "builds")

    def test_404_is_not_found(self):
        with self.assertRaises(NotFoundException):
            raise_for_error(response(404, {}), "builds")

    def test_is_project_does_not_exist_rejects_non_dict(self):
        self.assertFalse(tap_azure_devops.is_project_does_not_exist(["x"]))
        self.assertFalse(tap_azure_devops.is_project_does_not_exist(None))
