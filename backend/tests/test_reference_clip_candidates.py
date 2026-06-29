from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.reference_clip_candidates import (
    mark_dataset_clip_as_reference_candidate,
    reference_clip_candidates_root,
    resolve_unique_wav_path,
    transcript_filename_stem,
)


class ReferenceClipCandidateHelpersTests(unittest.TestCase):
    def test_transcript_filename_stem_uses_transcript_text(self) -> None:
        self.assertEqual(transcript_filename_stem("Hello world", "clip-1"), "Hello world")

    def test_transcript_filename_stem_strips_forbidden_chars_only(self) -> None:
        self.assertEqual(transcript_filename_stem('Say: "hello"', "clip-1"), "Say hello")

    def test_resolve_unique_wav_path_adds_suffix_for_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = resolve_unique_wav_path(root, "Hello world")
            first.write_bytes(b"wav")
            second = resolve_unique_wav_path(root, "Hello world")
            self.assertEqual(first.name, "Hello world.wav")
            self.assertEqual(second.name, "Hello world (2).wav")


class ReferenceClipCandidatePromotionTests(unittest.TestCase):
    @patch("app.reference_clip_candidates._get_dataset_run_for_project")
    @patch("app.reference_clip_candidates.get_candidate_review_media_path")
    def test_mark_dataset_clip_copies_audio_into_project_folder(
        self,
        get_media_path,
        validate_run,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            media_root = Path(temp_dir)
            source_path = media_root / "source.wav"
            source_path.write_bytes(b"RIFF")
            get_media_path.return_value = source_path

            class FakeRepository:
                def __init__(self) -> None:
                    self.media_root = media_root

                def get_project(self, project_id: str) -> None:
                    return None

            result = mark_dataset_clip_as_reference_candidate(
                FakeRepository(),
                project_id="project-1",
                dataset_run_id="dataset-run-1",
                clip_id="candidate_review_clip_000001",
                transcript_text="Listen carefully",
            )

            destination = reference_clip_candidates_root(media_root, "project-1") / "Listen carefully.wav"
            validate_run.assert_called_once()
            self.assertTrue(destination.exists())
            self.assertEqual(destination.read_bytes(), b"RIFF")
            self.assertEqual(result["filename"], "Listen carefully.wav")
            self.assertEqual(result["relative_path"], "reference-clip-candidates/project-1/Listen carefully.wav")


if __name__ == "__main__":
    unittest.main()
