from tests.http_support import LiveServerTestCase


class MediaRouteTests(LiveServerTestCase):
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
