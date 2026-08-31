import asyncio
import importlib.util
import json
import pathlib
import sys
import types
import unittest


SCRIPT_PATH = pathlib.Path(__file__).parents[1] / "scripts" / "fingerprint_gateway.py"
SPEC = importlib.util.spec_from_file_location("fingerprint_gateway", SCRIPT_PATH)
FINGERPRINT_GATEWAY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = FINGERPRINT_GATEWAY
SPEC.loader.exec_module(FINGERPRINT_GATEWAY)


class FakeTargetResponse:
    status_code = 700
    headers = {"content-type": "text/plain", "x-target-evidence": "invalid-status"}
    content = b"raw-target-body"


class FakeSession:
    async def request(self, *args, **kwargs):
        return FakeTargetResponse()


class FakeRequest:
    match_info = {"profile_id": "chrome"}
    headers = {
        "x-fingerprint-url": FINGERPRINT_GATEWAY.encode_metadata(
            "https://www.youtube.com/watch?v=invalid-status"
        ),
        "x-fingerprint-method": "GET",
        "x-fingerprint-headers": FINGERPRINT_GATEWAY.encode_metadata({}),
    }

    async def read(self):
        return b""


class FingerprintGatewayStatusTest(unittest.TestCase):
    def test_python_gateway_rejects_invalid_target_status_with_raw_evidence(self):
        gateway = FINGERPRINT_GATEWAY.Gateway()
        gateway.profiles["chrome"] = types.SimpleNamespace(
            user_agent="UA",
            semaphore=asyncio.Semaphore(1),
            session=FakeSession(),
            proxy_url="http://proxy.invalid",
            impersonate_target="chrome",
        )

        response = asyncio.run(gateway.fetch(FakeRequest()))
        self.assertEqual(response.status, 502)
        payload = json.loads(response.text)
        self.assertEqual(payload["failure_kind"], "invalid_target_status")
        self.assertEqual(payload["error_type"], "InvalidTargetHttpStatus")
        self.assertEqual(payload["target_status_raw"], 700)
        self.assertEqual(payload["target_body_sample_base64"], "cmF3LXRhcmdldC1ib2R5")
        self.assertEqual(
            payload["target_response_headers"],
            {"content-type": "text/plain", "x-target-evidence": "invalid-status"},
        )

    def test_python_gateway_treats_status_zero_as_proxy_transport(self):
        class ZeroStatusResponse:
            status_code = 0
            headers = {}
            content = b""

        class ZeroStatusSession:
            async def request(self, *args, **kwargs):
                return ZeroStatusResponse()

        gateway = FINGERPRINT_GATEWAY.Gateway()
        gateway.profiles["chrome"] = types.SimpleNamespace(
            user_agent="UA",
            semaphore=asyncio.Semaphore(1),
            session=ZeroStatusSession(),
            proxy_url="http://proxy.invalid",
            impersonate_target="chrome",
        )

        response = asyncio.run(gateway.fetch(FakeRequest()))
        self.assertEqual(response.status, 502)
        payload = json.loads(response.text)
        self.assertEqual(payload["failure_kind"], "proxy_transport")
        self.assertEqual(payload["target_status_raw"], 0)


if __name__ == "__main__":
    unittest.main()
