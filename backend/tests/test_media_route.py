import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.responses import FileResponse

from app.main import get_slice_media, get_variant_media, save_slice_state
from app.models import SliceSaveRequest
from app.repository import SliceSaveValidationError


def write_test_wav(path: Path) -> None:
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(48000)
        wav_file.writeframes(b"\x00\x00" * 4800)


class MediaRouteTests(TestCase):
    def test_variant_media_route_returns_file_response(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "variant-test.wav"
            write_test_wav(path)

            with patch("app.main.repository.get_variant_media_path", return_value=path):
                response = get_variant_media("variant-test")

            self.assertIsInstance(response, FileResponse)
            self.assertEqual(Path(response.path), path)
            self.assertEqual(response.media_type, "audio/wav")
            self.assertEqual(response.headers.get("accept-ranges"), "bytes")

    def test_slice_media_route_returns_file_response(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "slice-test.wav"
            write_test_wav(path)

            with patch("app.main.repository.get_slice_media_path", return_value=path):
                response = get_slice_media("slice-test")

            self.assertIsInstance(response, FileResponse)
            self.assertEqual(Path(response.path), path)
            self.assertEqual(response.media_type, "audio/wav")
            self.assertEqual(response.headers.get("accept-ranges"), "bytes")

    def test_save_slice_state_maps_value_error_to_bad_request(self) -> None:
        with patch(
            "app.main.repository.save_slice_state",
            side_effect=SliceSaveValidationError("message requires milestone or state change"),
        ):
            with self.assertRaises(HTTPException) as exc_info:
                save_slice_state("slice-test", SliceSaveRequest(message="note only"))

        self.assertEqual(exc_info.exception.status_code, 400)
        self.assertEqual(exc_info.exception.detail, "message requires milestone or state change")
