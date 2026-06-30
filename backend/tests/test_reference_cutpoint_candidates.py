import json
import math
import wave
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from sqlmodel import Session

from app.models import ProcessingRun, ProcessingRunStatus, ReferenceRunCreate, ReferenceRunStatus, ReferencePickerRun
from app.reference_acoustic_signature import ACOUSTIC_SIGNATURE_V2_ID
from app.repository import SQLiteRepository


def write_tone_wav(path: Path, *, sample_rate: int, duration_seconds: float) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame_count = int(sample_rate * duration_seconds)
    amplitude = int(32767 * 0.25)
    frequency = 220.0
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        for frame_index in range(frame_count):
            sample = int(amplitude * math.sin((2 * math.pi * frequency * frame_index) / sample_rate))
            wav_file.writeframes(sample.to_bytes(2, "little", signed=True))
    return frame_count


class ReferenceCutpointCandidateTests(TestCase):
    def setUp(self) -> None:
        self.temp_dir = TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.repository = SQLiteRepository(
            db_path=root / "project.db",
            legacy_seed_path=root / "missing-seed.json",
            media_root=root / "media",
            exports_root=root / "exports",
        )
        self.addCleanup(self.repository.close)

    def tearDown(self) -> None:
        self.repository.close()
        self.temp_dir.cleanup()

    def _seed_dataset_run_with_cutpoints(self) -> tuple[str, str]:
        source_path = self.repository.media_root / "sources" / "src-cutpoint.wav"
        frame_count = write_tone_wav(source_path, sample_rate=48000, duration_seconds=20.0)
        with Session(self.repository.engine, expire_on_commit=False) as session:
            from app.models import SourceRecording

            session.add(
                SourceRecording(
                    id="src-cutpoint",
                    batch_id="phase1-demo",
                    file_path=str(source_path),
                    sample_rate=48000,
                    num_channels=1,
                    num_samples=frame_count,
                )
            )
            run_id = "dataset-cutpoint-test"
            artifact_root = f"dataset-runs/phase1-demo/{run_id}"
            run_root = self.repository.media_root / artifact_root
            artifacts = run_root / "artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            session.add(
                ProcessingRun(
                    id=run_id,
                    project_id="phase1-demo",
                    artifact_root=artifact_root,
                    status=ProcessingRunStatus.COMPLETED,
                    input_summary={
                        "source_recording_ids": ["src-cutpoint"],
                        "source_wavs": [str(source_path)],
                        "single_speaker": True,
                    },
                )
            )
            session.commit()

        (artifacts / "source_audio_manifest.json").write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "source_audio_id": "source_audio_0000",
                            "source_recording_id": source_path.stem,
                            "path": str(source_path),
                            "sample_rate": 48000,
                            "num_channels": 1,
                            "num_samples": frame_count,
                            "duration_sec": 20.0,
                            "content_hash": "hash",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (artifacts / "audio_variants_manifest.json").write_text(
            json.dumps(
                {
                    "variants": [
                        {
                            "source_audio_id": "source_audio_0000",
                            "kind": "analysis_audio",
                            "analysis_sample_rate": 16000,
                            "source_sample_rate": 48000,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
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
        return run_id, "src-cutpoint"

    def test_reference_run_uses_cutpoint_overlap_assembly(self) -> None:
        dataset_run_id, recording_id = self._seed_dataset_run_with_cutpoints()
        created = self.repository.create_reference_run(
            "phase1-demo",
            ReferenceRunCreate(
                recording_ids=[recording_id],
                dataset_run_id=dataset_run_id,
                candidate_count_cap=12,
            ),
        )
        completed = self.repository.process_reference_run(created.id)
        self.assertEqual(completed.status, ReferenceRunStatus.COMPLETED)
        self.assertGreater(completed.candidate_count, 0)

        with Session(self.repository.engine, expire_on_commit=False) as session:
            run_row = session.get(ReferencePickerRun, created.id)
            self.assertIsNotNone(run_row)
            config = dict(run_row.config or {})
            manifest = json.loads((Path(run_row.artifact_root) / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(config.get("candidate_assembly_backend"), "cutpoint_overlap")
        self.assertEqual(config.get("embedding_space_id"), ACOUSTIC_SIGNATURE_V2_ID)
        self.assertEqual(manifest["embedding_extractor"], "acoustic_signature_v2")

        candidates = self.repository.list_reference_run_candidates(created.id, limit=12)
        self.assertGreater(len(candidates), 0)
        self.assertTrue(all(candidate.transcript_text for candidate in candidates))
        self.assertTrue(all(candidate.embedding_space_id == ACOUSTIC_SIGNATURE_V2_ID for candidate in candidates))
        cluster_flags = [flag for candidate in candidates for flag in candidate.risk_flags if flag.startswith("cluster_")]
        self.assertTrue(cluster_flags)
        self.assertGreater(len(set(cluster_flags)), 0)


if __name__ == "__main__":
    unittest.main()
