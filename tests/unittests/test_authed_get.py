import json
import unittest
from unittest import mock

import requests

import tap_azure_devops
from tap_azure_devops import (
    RetriableServerError,
    authed_get,
    ensure_json_body,
)


def response(status, content, url="https://vsaex.dev.azure.com/org/_apis/userentitlements"):
    result = requests.Response()
    result.status_code = status
    result.url = url
    result._content = content
    return result


TRUNCATED_BODY = json.dumps({"items": [{"id": "a" * 50}]}).encode()[:-10]


class TestEnsureJsonBody(unittest.TestCase):
    def test_valid_json_passes(self):
        ensure_json_body(response(200, b'{"items": []}'), "user_entitlements")

    def test_truncated_json_raises_retriable_error(self):
        with self.assertRaises(RetriableServerError) as ctx:
            ensure_json_body(response(200, TRUNCATED_BODY), "user_entitlements")
        self.assertIn("Malformed JSON body for user_entitlements", str(ctx.exception))
        self.assertIn("/_apis/userentitlements", str(ctx.exception))


@mock.patch("tap_azure_devops.time.sleep", return_value=None)
@mock.patch("tap_azure_devops.get_auth_header", return_value={"Authorization": "Basic x"})
@mock.patch.object(tap_azure_devops.session, "request")
class TestAuthedGetMalformedJson(unittest.TestCase):
    def setUp(self):
        tap_azure_devops.config_data = {"access_token": "t"}

    def test_truncated_200_is_retried_then_succeeds(self, request, _auth, _sleep):
        good = response(200, b'{"items": [], "continuationToken": null}')
        request.side_effect = [response(200, TRUNCATED_BODY), good]
        resp = authed_get("user_entitlements", good.url, {"top": 100})
        self.assertIs(resp, good)
        self.assertEqual(request.call_count, 2)

    def test_truncated_200_gives_up_after_max_tries(self, request, _auth, _sleep):
        request.side_effect = [response(200, TRUNCATED_BODY)] * 10
        with self.assertRaises(RetriableServerError):
            authed_get("user_entitlements", "https://vsaex.dev.azure.com/org/_apis/userentitlements")
        self.assertEqual(request.call_count, 10)

    def test_empty_200_is_retried_then_succeeds(self, request, _auth, _sleep):
        good = response(200, b'{"items": []}')
        request.side_effect = [response(200, b""), good]
        resp = authed_get("user_entitlements", good.url, {"top": 100})
        self.assertIs(resp, good)
        self.assertEqual(request.call_count, 2)

    def test_empty_201_is_returned_without_retry(self, request, _auth, _sleep):
        created = response(201, b"", url="https://dev.azure.com/org/_apis/wit/wiql")
        request.return_value = created
        resp = authed_get("work_items", created.url, method="post", json_body={"query": "x"})
        self.assertIs(resp, created)
        self.assertEqual(request.call_count, 1)

    def test_streamed_response_body_is_not_parsed(self, request, _auth, _sleep):
        blob = response(200, b"\x00binary", url="https://dev.azure.com/org/_apis/git/blobs/x")
        request.return_value = blob
        resp = authed_get("blob_content", blob.url, stream=True)
        self.assertIs(resp, blob)
        self.assertEqual(request.call_count, 1)
