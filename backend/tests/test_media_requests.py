from tests.http_support import LiveServerTestCase


class MediaRequestTests(LiveServerTestCase):
    def test_dataset_processing_status_endpoint_returns_run_progress(self) -> None:
        review_windows = self.client.get("/api/source-recordings/src-001/review-windows").json()
        self.assertGreaterEqual(len(review_windows), 1)

        start_response = self.client.post(
            "/api/source-recordings/src-001/dataset-processing",
            json={"review_window_ids": [review_windows[0]["id"]]},
        )

        self.assertEqual(start_response.status_code, 202)
        start_payload = start_response.json()
        self.assertEqual(start_payload["source_recording_id"], "src-001")
        self.assertEqual(start_payload["status"], "asr_running")
        self.assertEqual(start_payload["phase"], "asr")
        self.assertEqual(start_payload["total_review_windows"], 1)
        self.assertEqual(start_payload["asr_total"], 1)
        self.assertEqual(start_payload["alignment_total"], 0)
        self.assertFalse(start_payload["health_page_ready"])

        status_response = self.client.get("/api/source-recordings/src-001/processing-status")

        self.assertEqual(status_response.status_code, 200)
        status_payload = status_response.json()
        self.assertEqual(status_payload["id"], start_payload["id"])
        self.assertEqual(status_payload["status"], "asr_running")
        self.assertEqual(status_payload["phase"], "asr")
        self.assertEqual(status_payload["asr_total"], 1)
        self.assertEqual(status_payload["asr_completed"], 0)
        self.assertEqual(status_payload["asr_failed"], 0)
        self.assertEqual(status_payload["alignment_total"], 0)
        self.assertEqual(status_payload["alignment_completed"], 0)
        self.assertEqual(status_payload["alignment_failed"], 0)
        self.assertFalse(status_payload["health_page_ready"])

    def test_variant_media_route_serves_audio_file(self) -> None:
        slices = self.client.get("/api/projects/phase1-demo/slices").json()
        detail = self.client.get(f"/api/slices/{slices[0]['id']}").json()

        response = self.client.get(f"/media/variants/{detail['active_variant_id']}.wav")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "audio/wav")
        self.assertEqual(response.headers.get("accept-ranges"), "bytes")
        self.assertGreater(len(response.content), 0)
        self.assertEqual(response.content[:4], b"RIFF")

    def test_slice_media_route_serves_audio_file(self) -> None:
        slices = self.client.get("/api/projects/phase1-demo/slices").json()
        slice_id = slices[0]["id"]

        response = self.client.get(f"/media/slices/{slice_id}.wav")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "audio/wav")
        self.assertEqual(response.headers.get("accept-ranges"), "bytes")
        self.assertGreater(len(response.content), 0)
        self.assertEqual(response.content[:4], b"RIFF")

    def test_save_slice_state_returns_bad_request_for_validation_error(self) -> None:
        response = self.client.post("/api/clips/clip-001/save", json={"message": "note only"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "message requires milestone or state change")
