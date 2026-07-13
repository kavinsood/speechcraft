"""Unit tests for the pure multi-file diarization helpers (option C).

These exercise the deterministic concat-offset planning and region remapping
that make speaker identity consistent across files. NeMo is imported lazily
inside the orchestrator, so these import cleanly without ML deps.
"""

import unittest

from speechcraft_dataset.diarization import (
    offset_vad_for_concat,
    plan_concat_layout,
    remap_concat_regions_to_sources,
)


class PlanConcatLayoutTest(unittest.TestCase):
    def test_cumulative_offsets_with_gap(self):
        variants = [
            {"source_audio_id": "source_audio_0000", "path": "a.wav", "analysis_duration_sec": 10.0},
            {"source_audio_id": "source_audio_0001", "path": "b.wav", "analysis_duration_sec": 5.0},
        ]
        layout = plan_concat_layout(variants, gap_sec=1.0)
        self.assertEqual(layout[0]["start_sec"], 0.0)
        self.assertEqual(layout[0]["end_sec"], 10.0)
        # second file starts after the first + the 1s gap
        self.assertEqual(layout[1]["start_sec"], 11.0)
        self.assertEqual(layout[1]["end_sec"], 16.0)


class OffsetVadTest(unittest.TestCase):
    def test_segments_shifted_into_concat_time(self):
        layout = [
            {"source_audio_id": "source_audio_0000", "path": "a.wav", "start_sec": 0.0, "end_sec": 10.0},
            {"source_audio_id": "source_audio_0001", "path": "b.wav", "start_sec": 11.0, "end_sec": 16.0},
        ]
        vad = [
            {"id": "v0", "source_audio_id": "source_audio_0000", "analysis_start_sec": 1.0, "analysis_end_sec": 2.0},
            {"id": "v1", "source_audio_id": "source_audio_0001", "analysis_start_sec": 0.5, "analysis_end_sec": 1.5},
        ]
        out = offset_vad_for_concat(vad, layout)
        self.assertEqual(out[0]["analysis_start_sec"], 1.0)
        self.assertEqual(out[1]["analysis_start_sec"], 11.5)
        self.assertEqual(out[1]["analysis_end_sec"], 12.5)
        self.assertTrue(all(r["source_audio_id"] == "concat" for r in out))


class RemapRegionsTest(unittest.TestCase):
    def setUp(self):
        self.layout = [
            {"source_audio_id": "source_audio_0000", "path": "a.wav", "start_sec": 0.0, "end_sec": 10.0},
            {"source_audio_id": "source_audio_0001", "path": "b.wav", "start_sec": 11.0, "end_sec": 16.0},
        ]

    def test_regions_mapped_back_to_local_coordinates(self):
        regions = [
            {"speaker_id": "speaker_0", "start_sec": 2.0, "end_sec": 4.0},
            {"speaker_id": "speaker_1", "start_sec": 12.0, "end_sec": 13.0},
        ]
        rows = remap_concat_regions_to_sources(regions, self.layout, sample_rate=16000)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["source_audio_id"], "source_audio_0000")
        self.assertEqual(rows[0]["start_sec"], 2.0)
        self.assertEqual(rows[0]["end_sec"], 4.0)
        # second region -> local time within file b (12-11=1s .. 2s)
        self.assertEqual(rows[1]["source_audio_id"], "source_audio_0001")
        self.assertEqual(rows[1]["start_sec"], 1.0)
        self.assertEqual(rows[1]["end_sec"], 2.0)
        self.assertEqual(rows[1]["start_sample"], 16000)

    def test_regions_in_gap_are_dropped(self):
        regions = [{"speaker_id": "speaker_0", "start_sec": 10.2, "end_sec": 10.8}]  # inside the gap
        rows = remap_concat_regions_to_sources(regions, self.layout, sample_rate=16000)
        self.assertEqual(rows, [])

    def test_speaker_identity_preserved_across_files(self):
        # Same global speaker id appears in both files -> stays one identity.
        regions = [
            {"speaker_id": "speaker_0", "start_sec": 1.0, "end_sec": 3.0},
            {"speaker_id": "speaker_0", "start_sec": 12.0, "end_sec": 14.0},
        ]
        rows = remap_concat_regions_to_sources(regions, self.layout, sample_rate=16000)
        self.assertEqual({r["speaker_id"] for r in rows}, {"speaker_0"})
        self.assertEqual({r["source_audio_id"] for r in rows}, {"source_audio_0000", "source_audio_0001"})


if __name__ == "__main__":
    unittest.main()
