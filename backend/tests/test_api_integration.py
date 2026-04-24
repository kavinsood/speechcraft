from __future__ import annotations

import io
import wave

from tests.http_support import LiveServerTestCase


def make_wav_bytes() -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(b"\x00\x00" * 160)
    return buffer.getvalue()


class ApiIntegrationTests(LiveServerTestCase):
    def test_healthz_route_returns_ok_json(self) -> None:
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})
        self.assertEqual(response.headers["content-type"], "application/json")

    def test_project_routes_return_seeded_demo_project(self) -> None:
        projects_response = self.client.get("/api/projects")
        self.assertEqual(projects_response.status_code, 200)
        projects = projects_response.json()
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["id"], "phase1-demo")

        slices_response = self.client.get("/api/projects/phase1-demo/slices")
        self.assertEqual(slices_response.status_code, 200)
        slices = slices_response.json()
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0]["id"], "clip-001")

        preview_response = self.client.get("/api/projects/phase1-demo/export-preview")
        self.assertEqual(preview_response.status_code, 200)
        preview = preview_response.json()
        self.assertEqual(preview["project_id"], "phase1-demo")
        self.assertEqual(preview["accepted_slice_count"], 0)

    def test_missing_project_route_returns_404(self) -> None:
        response = self.client.get("/api/projects/missing-project/export-preview")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Project not found")

    def test_project_preparation_enqueues_job_without_mutating_raw_recordings(self) -> None:
        before_response = self.client.get("/api/projects/phase1-demo/source-recordings")
        self.assertEqual(before_response.status_code, 200)
        before_recordings = before_response.json()
        raw_recordings = [recording for recording in before_recordings if recording["parent_recording_id"] is None]
        self.assertGreaterEqual(len(raw_recordings), 1)

        response = self.client.post(
            "/api/projects/phase1-demo/preparation",
            json={"target_sample_rate": 16000, "channel_mode": "mono"},
        )

        self.assertEqual(response.status_code, 202)
        payload = response.json()
        self.assertEqual(payload["job"]["kind"], "preprocess")
        self.assertEqual(payload["job"]["status"], "pending")
        self.assertEqual(payload["created_recordings"], [])

        after_response = self.client.get("/api/projects/phase1-demo/source-recordings")
        self.assertEqual(after_response.status_code, 200)
        after_recordings = after_response.json()
        after_raw_recordings = [
            recording for recording in after_recordings if recording["parent_recording_id"] is None
        ]
        self.assertEqual(
            [recording["id"] for recording in after_raw_recordings],
            [recording["id"] for recording in raw_recordings],
        )

    def test_project_upload_stream_creates_raw_source_recording(self) -> None:
        create_response = self.client.post(
            "/api/import-batches",
            json={"id": "upload-demo", "name": "Upload Demo"},
        )
        self.assertEqual(create_response.status_code, 200)

        upload_response = self.client.post(
            "/api/projects/upload-demo/source-recordings/upload",
            files={"file": ("sample.wav", make_wav_bytes(), "audio/wav")},
        )

        self.assertEqual(upload_response.status_code, 200)
        recording = upload_response.json()
        self.assertEqual(recording["batch_id"], "upload-demo")
        self.assertIsNone(recording["parent_recording_id"])
        self.assertEqual(recording["sample_rate"], 16000)
        self.assertEqual(recording["num_channels"], 1)
        self.assertEqual(recording["num_samples"], 160)

        recordings_response = self.client.get("/api/projects/upload-demo/source-recordings")
        self.assertEqual(recordings_response.status_code, 200)
        self.assertEqual([item["id"] for item in recordings_response.json()], [recording["id"]])

    def test_project_upload_rejects_non_wav_extension(self) -> None:
        create_response = self.client.post(
            "/api/import-batches",
            json={"id": "upload-invalid", "name": "Upload Invalid"},
        )
        self.assertEqual(create_response.status_code, 200)

        upload_response = self.client.post(
            "/api/projects/upload-invalid/source-recordings/upload",
            files={"file": ("sample.mp3", b"not audio", "audio/mpeg")},
        )

        self.assertEqual(upload_response.status_code, 400)
        self.assertEqual(upload_response.json()["detail"], "Only WAV files are supported right now")

    def test_cors_preflight_uses_explicit_origin_for_allowed_frontend(self) -> None:
        response = self.client.options(
            "/api/projects",
            headers={
                "Origin": "http://127.0.0.1:5173",
                "Access-Control-Request-Method": "GET",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("access-control-allow-origin"), "http://127.0.0.1:5173")
        self.assertEqual(response.headers.get("access-control-allow-credentials"), "true")
        self.assertNotEqual(response.headers.get("access-control-allow-origin"), "*")

    def test_cors_preflight_rejects_unknown_origin(self) -> None:
        response = self.client.options(
            "/api/projects",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "GET",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Disallowed CORS origin", response.text)
        self.assertNotIn("access-control-allow-origin", response.headers)
