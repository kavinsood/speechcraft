import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.reference_cutpoint_assembly import (
    generate_overlapping_cutpoint_windows,
    load_cutpoint_windows,
    reconstruct_transcript,
)


class ReferenceCutpointAssemblyTests(unittest.TestCase):
    def test_generate_overlapping_cutpoint_windows_strides(self) -> None:
        cutpoints = [{"id": f"cut-{index}", "cut_local_sample": index * 16000} for index in range(8)]
        windows = generate_overlapping_cutpoint_windows(
            cutpoints,
            sample_rate=16000,
            min_sec=3.0,
            max_sec=8.0,
            target_sec=4.5,
            stride_cutpoints=2,
        )
        self.assertGreaterEqual(len(windows), 2)
        starts = [window[0] for window in windows]
        self.assertIn(0, starts)
        self.assertIn(32000, starts)

    def test_reconstruct_transcript_dedupes_raw_token_ids(self) -> None:
        text = reconstruct_transcript(
            [
                {"word": "hello", "raw_token": "hello", "raw_token_id": "t1"},
                {"word": "hello", "raw_token": "hello", "raw_token_id": "t1"},
                {"word": "world", "raw_token": "world", "raw_token_id": "t2"},
            ]
        )
        self.assertEqual(text, "hello world")

    def test_load_cutpoint_windows_reads_dataset_artifacts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            run_root = Path(temp_dir)
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "processing_buffers.json").write_text(
                json.dumps(
                    [
                        {
                            "buffer_id": "buffer_000000",
                            "source_audio_id": "source_audio_0000",
                            "source_start_sample": 0,
                            "sample_rate": 16000,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            cutpoint_rows = []
            for index in range(10):
                sample = index * 16000
                cutpoint_rows.append(
                    json.dumps(
                        {
                            "id": f"buffer_000000-cut-{index:04d}",
                            "buffer_id": "buffer_000000",
                            "cut_local_sample": sample,
                            "source_sample": sample,
                        }
                    )
                )
            (artifacts / "safe_cutpoints.jsonl").write_text("\n".join(cutpoint_rows), encoding="utf-8")
            (artifacts / "aligned_words.jsonl").write_text(
                "\n".join(
                    json.dumps(
                        {
                            "buffer_id": "buffer_000000",
                            "word": f"word{index}",
                            "raw_token": f"word{index}",
                            "source_start_sample": index * 16000 + 2000,
                            "source_end_sample": index * 16000 + 4000,
                        }
                    )
                    for index in range(8)
                ),
                encoding="utf-8",
            )
            windows = load_cutpoint_windows(
                run_root,
                target_durations=[4.5],
                stride_cutpoints=2,
            )
            self.assertGreater(len(windows), 0)
            self.assertTrue(any(window.transcript_text for window in windows))


if __name__ == "__main__":
    unittest.main()
