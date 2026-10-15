import json
import unittest
from unittest import mock

import requests

from tap_azure_devops import nango


def response(status, body=None, reason=None):
    result = requests.Response()
    result.status_code = status
    result.reason = reason
    if body is not None:
        result._content = json.dumps(body).encode()
    else:
        result._content = b""
    return result


def ok_response(pat="pat-1"):
    return response(200, {"credentials": {"type": "BASIC", "password": pat}})


def config(**extra):
    cfg = {"nango_connection_id": "conn-1", "nango_secret_key": "secret"}
    cfg.update(extra)
    return cfg


@mock.patch("time.sleep", return_value=None)
@mock.patch("tap_azure_devops.nango.requests.get")
class TestGetNangoToken(unittest.TestCase):
    def urls(self, get):
        return [call.kwargs.get("url") or call.args[0] for call in get.call_args_list]

    def test_success(self, get, _sleep):
        get.return_value = ok_response()

        self.assertEqual(nango.get_nango_token(config()), "pat-1")
        self.assertEqual(get.call_count, 1)
        url = self.urls(get)[0]
        self.assertIn("/connection/conn-1?provider_config_key=azure-devops", url)
        self.assertNotIn("force_refresh", url)
        self.assertEqual(get.call_args.kwargs["timeout"], nango.NANGO_REQUEST_TIMEOUT)
        _sleep.assert_not_called()

    def test_provider_config_key_override(self, get, _sleep):
        get.return_value = ok_response()

        nango.get_nango_token(config(nango_provider_config_key="azure-devops-2"))

        self.assertIn("provider_config_key=azure-devops-2", self.urls(get)[0])

    def test_400_raises_with_nango_error_details(self, get, _sleep):
        get.return_value = response(
            400, {"error": {"code": "invalid_credentials", "message": "token revoked"}}
        )

        with self.assertRaises(nango.NangoRefreshError) as ctx:
            nango.get_nango_token(config())

        self.assertEqual(get.call_count, 1)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("invalid_credentials: token revoked", str(ctx.exception))
        self.assertIn("conn-1", str(ctx.exception))
        _sleep.assert_not_called()

    def test_401_and_404_are_not_retried(self, get, _sleep):
        for status in (401, 404):
            get.reset_mock()
            get.return_value = response(status, {"error": {"code": "unknown_connection"}})

            with self.assertRaises(nango.NangoRefreshError):
                nango.get_nango_token(config())

            self.assertEqual(get.call_count, 1)

    def test_5xx_is_retried_then_succeeds(self, get, _sleep):
        get.side_effect = [
            response(503, reason="Service Temporarily Unavailable"),
            response(504, reason="Gateway Time-out"),
            ok_response(),
        ]

        self.assertEqual(nango.get_nango_token(config()), "pat-1")
        self.assertEqual(get.call_count, 3)

    def test_5xx_gives_up_after_max_tries(self, get, _sleep):
        get.return_value = response(503, reason="Service Temporarily Unavailable")

        with self.assertRaises(nango.NangoServerError) as ctx:
            nango.get_nango_token(config())

        self.assertEqual(get.call_count, nango.NANGO_MAX_TRIES)
        self.assertIn("503", str(ctx.exception))
        self.assertIn("Service Temporarily Unavailable", str(ctx.exception))

    def test_429_is_retried_after_retry_after(self, get, sleep):
        limited = response(429, reason="Too Many Requests")
        limited.headers["Retry-After"] = "7"
        get.side_effect = [limited, ok_response()]

        self.assertEqual(nango.get_nango_token(config()), "pat-1")
        self.assertEqual(get.call_count, 2)
        sleep.assert_any_call(7)

    def test_429_gives_up_after_max_tries(self, get, _sleep):
        get.return_value = response(429, reason="Too Many Requests")

        with self.assertRaises(nango.NangoServerError) as ctx:
            nango.get_nango_token(config())

        self.assertEqual(get.call_count, nango.NANGO_MAX_TRIES)
        self.assertIn("429", str(ctx.exception))

    def test_timeout_connection_and_chunked_encoding_errors_are_retried(
        self, get, _sleep
    ):
        get.side_effect = [
            requests.Timeout("timed out"),
            requests.ConnectionError("reset"),
            requests.exceptions.ChunkedEncodingError("Connection broken"),
            ok_response(),
        ]

        self.assertEqual(nango.get_nango_token(config()), "pat-1")
        self.assertEqual(get.call_count, 4)

    def test_missing_config_raises_before_any_request(self, get, _sleep):
        with self.assertRaises(ValueError):
            nango.get_nango_token({"nango_connection_id": "conn-1"})
        with self.assertRaises(ValueError):
            nango.get_nango_token({"nango_secret_key": "secret"})
        get.assert_not_called()


class TestRetryAfterSeconds(unittest.TestCase):
    def test_caps_and_defaults(self):
        resp = response(429)
        self.assertEqual(nango.retry_after_seconds(resp), 0)
        resp.headers["Retry-After"] = "600"
        self.assertEqual(nango.retry_after_seconds(resp), nango.NANGO_MAX_RETRY_AFTER)
        resp.headers["Retry-After"] = "Wed, 21 Oct 2026 07:28:00 GMT"
        self.assertEqual(nango.retry_after_seconds(resp), 0)


class TestNangoErrorDetails(unittest.TestCase):
    def test_prefers_code_and_message(self):
        resp = response(400, {"error": {"code": "invalid_credentials", "message": "nope"}})
        self.assertEqual(nango.nango_error_details(resp), "invalid_credentials: nope")

    def test_string_error(self):
        self.assertEqual(
            nango.nango_error_details(response(400, {"error": "Bad Request"})),
            "Bad Request",
        )

    def test_non_json_body_falls_back_to_reason_without_leaking_body(self):
        resp = requests.Response()
        resp.status_code = 503
        resp.reason = "Service Temporarily Unavailable"
        resp._content = b"<html><body>nginx</body></html>"
        self.assertEqual(
            nango.nango_error_details(resp), "Service Temporarily Unavailable"
        )

    def test_connection_payload_without_error_is_not_echoed(self):
        resp = response(400, {"end_user": {"email": "someone@example.com"}}, reason="Bad Request")
        self.assertEqual(nango.nango_error_details(resp), "Bad Request")
