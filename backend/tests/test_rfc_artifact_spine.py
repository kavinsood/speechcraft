from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import TestCase

from sqlmodel import Session, SQLModel, select

from app.models import (
    CandidateClip,
    DatasetAudioVariant,
    JobKind,
    QCDecisionBucket,
    RFC_PIPELINE_VERSION,
    RFC_PRETRAINING_JOB_DAG,
    ProcessingRun,
    ProcessingRunStatus,
    RfcStage,
    RunArtifact,
    RunArtifactKind,
    RunArtifactStatus,
    SourceAudio,
    resolve_run_artifact_path,
)
from app.repository import SQLiteRepository


class RfcArtifactSpineTests(TestCase):
    def test_rfc_coarse_tables_are_registered_and_high_cardinality_tables_are_absent(self) -> None:
        coarse_tables = {
            "processingrun",
            "sourceaudio",
            "datasetaudiovariant",
            "sourcehealthscan",
            "runartifact",
            "speakeridentity",
            "candidateclip",
            "targetspeakerprofile",
            "speakerpuritymetrics",
            "qcdecision",
            "exportmanifest",
        }
        high_cardinality_tables = {
            "vadsegment",
            "speakerregion",
            "trustedregion",
            "trustedchunk",
            "processingbuffer",
            "asrtranscript",
            "normalizedtranscript",
            "normalizedtoken",
            "alignedword",
            "safecutpoint",
            "sliceablecore",
        }

        self.assertTrue(coarse_tables.issubset(SQLModel.metadata.tables))
        self.assertTrue(high_cardinality_tables.isdisjoint(SQLModel.metadata.tables))

        with TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            repository = SQLiteRepository(
                db_path=temp_dir / "project.db",
                legacy_seed_path=temp_dir / "missing-seed.json",
                media_root=temp_dir / "media",
                exports_root=temp_dir / "exports",
            )
            try:
                with Session(repository.engine) as session:
                    source_audio = SourceAudio(
                        id="source-audio-1",
                        source_recording_id="recording-1",
                        file_path="/tmp/source.wav",
                        sample_rate=16000,
                        num_channels=1,
                        num_samples=16000,
                        duration_sec=1.0,
                    )
                    session.add(source_audio)
                    session.commit()

                    stored = session.exec(select(SourceAudio)).one()
                    self.assertEqual(stored.pipeline_version, RFC_PIPELINE_VERSION)
            finally:
                repository.close()

    def test_file_backed_run_artifact_registers_hashes_backend_and_summary(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            repository = SQLiteRepository(
                db_path=temp_dir / "project.db",
                legacy_seed_path=temp_dir / "missing-seed.json",
                media_root=temp_dir / "media",
                exports_root=temp_dir / "exports",
            )
            try:
                with Session(repository.engine) as session:
                    run = ProcessingRun(
                        id="run-1",
                        project_id="project-1",
                        artifact_root="runs/run-1",
                        stage=RfcStage.MFA,
                        status=ProcessingRunStatus.COMPLETED,
                        config_hash="cfg-123",
                    )
                    artifact = RunArtifact(
                        id="artifact-1",
                        run_id=run.id,
                        project_id=run.project_id,
                        kind=RunArtifactKind.ALIGNED_WORDS_JSONL,
                        path="artifacts/aligned_words.jsonl",
                        content_hash="sha256:abc",
                        byte_size=4096,
                        config_hash=run.config_hash,
                        input_artifact_hashes={"mfa_textgrid": "sha256:textgrid"},
                        backend="mfa",
                        backend_version="english_mfa",
                        status=RunArtifactStatus.MATERIALIZED,
                        summary={"word_count": 1234},
                        reason_codes=[],
                    )
                    session.add(run)
                    session.add(artifact)
                    session.commit()

                    stored = session.exec(select(RunArtifact)).one()
                    self.assertEqual(stored.run_id, "run-1")
                    self.assertEqual(stored.kind, RunArtifactKind.ALIGNED_WORDS_JSONL)
                    self.assertEqual(stored.path, "artifacts/aligned_words.jsonl")
                    self.assertEqual(stored.content_hash, "sha256:abc")
                    self.assertEqual(stored.byte_size, 4096)
                    self.assertEqual(stored.config_hash, "cfg-123")
                    self.assertEqual(stored.input_artifact_hashes["mfa_textgrid"], "sha256:textgrid")
                    self.assertEqual(stored.backend, "mfa")
                    self.assertEqual(stored.summary["word_count"], 1234)
            finally:
                repository.close()

    def test_run_artifact_paths_are_relative_to_run_root(self) -> None:
        def build_artifact(path: str) -> RunArtifact:
            return RunArtifact(
                id=f"artifact-{path}",
                run_id="run-1",
                project_id="project-1",
                kind=RunArtifactKind.WORKER_LOG,
                path=path,
            )

        self.assertEqual(build_artifact("logs/dataset_worker.log").path, "logs/dataset_worker.log")

        for invalid_path in (
            "",
            "/tmp/run/log.txt",
            "../outside.json",
            "artifacts/../outside.json",
            "artifacts//file.json",
            "artifacts/.",
            "artifacts/./file.json",
            "artifacts/",
            "C:/temp/file.json",
            "./artifact.json",
            "~/artifact.json",
            "logs\\worker.log",
        ):
            with self.subTest(path=invalid_path):
                with self.assertRaises(ValueError):
                    build_artifact(invalid_path)

    def test_processing_run_artifact_root_is_storage_root_relative(self) -> None:
        run = ProcessingRun(
            id="run-1",
            project_id="project-1",
            artifact_root="runs/project-1/run-1",
            stage=RfcStage.INGEST,
        )

        self.assertEqual(run.artifact_root, "runs/project-1/run-1")

        for invalid_root in (
            "",
            "/tmp/speechcraft/run-1",
            "../run-1",
            "runs//run-1",
            "runs/./run-1",
            "runs/run-1/",
            "C:/speechcraft/run-1",
            "~/speechcraft/run-1",
        ):
            with self.subTest(path=invalid_root):
                with self.assertRaises(ValueError):
                    ProcessingRun(
                        id=f"run-{invalid_root}",
                        project_id="project-1",
                        artifact_root=invalid_root,
                        stage=RfcStage.INGEST,
                    )

    def test_resolve_run_artifact_path_stays_under_storage_root(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            storage_root = Path(temp_dir_raw) / "storage"
            resolved = resolve_run_artifact_path(
                storage_root,
                "runs/project-1/run-1",
                "artifacts/aligned_words.jsonl",
            )

            self.assertEqual(
                resolved,
                storage_root.resolve() / "runs" / "project-1" / "run-1" / "artifacts" / "aligned_words.jsonl",
            )

            for invalid_root, invalid_path in (
                ("../runs/run-1", "artifacts/file.json"),
                ("runs/run-1", "../file.json"),
                ("runs/run-1", "artifacts/../file.json"),
                ("runs/run-1", "C:/temp/file.json"),
            ):
                with self.subTest(root=invalid_root, path=invalid_path):
                    with self.assertRaises(ValueError):
                        resolve_run_artifact_path(storage_root, invalid_root, invalid_path)

    def test_resolve_run_artifact_path_rejects_symlink_escape(self) -> None:
        with TemporaryDirectory() as temp_dir_raw:
            temp_dir = Path(temp_dir_raw)
            storage_root = temp_dir / "storage"
            outside = temp_dir / "outside"
            run_dir = storage_root / "runs" / "run-1"
            outside.mkdir(parents=True)
            run_dir.mkdir(parents=True)
            (run_dir / "escape").symlink_to(outside, target_is_directory=True)

            with self.assertRaises(ValueError):
                resolve_run_artifact_path(storage_root, "runs/run-1", "escape/file.json")

    def test_dataset_artifact_kinds_cover_worker_outputs(self) -> None:
        expected = {
            "source_audio_manifest_json",
            "source_audio_summary_json",
            "audio_variants_manifest_json",
            "audio_variants_summary_json",
            "preflight_json",
            "run_config_json",
            "run_status_json",
            "runtime_versions_json",
            "worker_log",
            "worker_process_log",
            "vad_segments_jsonl",
            "vad_summary_json",
            "speaker_regions_jsonl",
            "speaker_regions_summary_json",
            "speaker_samples_manifest_json",
            "speaker_selection_json",
            "processing_buffers_json",
            "processing_buffer_summary_json",
            "asr_mfa_queue_json",
            "asr_mfa_queue_summary_json",
            "rejected_buffers_json",
            "asr_transcripts_json",
            "asr_transcripts_summary_json",
            "transcript_hazards_json",
            "symbol_hazard_summary_json",
            "normalized_transcripts_json",
            "normalization_summary_json",
            "mfa_textgrid",
            "aligned_words_jsonl",
            "aligned_words_summary_json",
            "alignment_qc_json",
            "alignment_qc_by_buffer_json",
            "alignment_qc_summary_json",
            "safe_cutpoints_jsonl",
            "safe_cutpoint_candidates_jsonl",
            "safe_cutpoint_summary_json",
            "candidate_review_manifest_json",
            "candidate_review_rejected_json",
            "candidate_review_summary_json",
            "clip_lab_state_json",
            "export_manifest_json",
            "export_audit_json",
            "export_summary_json",
            "quality_dropped_json",
            "voxcpm_manifest_jsonl",
        }

        self.assertTrue(expected.issubset({kind.value for kind in RunArtifactKind}))

    def test_rfc_job_dag_and_decision_enums_cover_product_path(self) -> None:
        stage_names = [stage.value for stage, _dependencies in RFC_PRETRAINING_JOB_DAG]

        self.assertEqual(
            stage_names,
            [
                "ingest",
                "audio_variants",
                "source_health",
                "vad",
                "diarization",
                "speaker_identity",
                "trusted_regions",
                "processing_buffers",
                "asr",
                "normalization",
                "mfa",
                "safe_cutpoints",
                "candidate_clips",
                "speaker_purity",
                "dataset_qc",
                "export",
            ],
        )
        self.assertEqual(JobKind.SOURCE_HEALTH_SCAN.value, "source_health_scan")
        self.assertEqual(JobKind.MFA_ALIGNMENT.value, "mfa_alignment")
        self.assertEqual(JobKind.SAFE_CUTPOINT_SLICING.value, "safe_cutpoint_slicing")
        self.assertEqual(QCDecisionBucket.ACCEPTED.value, "accepted")
        self.assertEqual(QCDecisionBucket.NEEDS_REVIEW.value, "needs_review")
        self.assertEqual(QCDecisionBucket.REJECTED.value, "rejected")
        self.assertEqual(CandidateClip.__tablename__, "candidateclip")
        self.assertEqual(DatasetAudioVariant.__tablename__, "datasetaudiovariant")

    def test_rfc_job_dag_dependencies_exist_and_are_acyclic(self) -> None:
        dag = {stage: dependencies for stage, dependencies in RFC_PRETRAINING_JOB_DAG}
        self.assertEqual(set(dag), set(RfcStage))

        for stage, dependencies in dag.items():
            for dependency in dependencies:
                self.assertIn(dependency, dag, f"{stage.value} depends on unknown {dependency.value}")

        visiting: set[RfcStage] = set()
        visited: set[RfcStage] = set()

        def visit(stage: RfcStage) -> None:
            if stage in visited:
                return
            self.assertNotIn(stage, visiting, f"cycle detected at {stage.value}")
            visiting.add(stage)
            for dependency in dag[stage]:
                visit(dependency)
            visiting.remove(stage)
            visited.add(stage)

        for stage in dag:
            visit(stage)

    def test_candidate_clip_uses_namespaced_artifact_refs_and_sample_indices(self) -> None:
        fields = CandidateClip.model_fields

        self.assertIn("processing_buffer_artifact_id", fields)
        self.assertIn("processing_buffer_ref", fields)
        self.assertIn("cutpoint_artifact_id", fields)
        self.assertIn("start_cutpoint_ref", fields)
        self.assertIn("end_cutpoint_ref", fields)
        self.assertIn("aligned_words_artifact_id", fields)
        self.assertIn("word_ids", fields)
        self.assertIn("source_start_sample", fields)
        self.assertIn("source_end_sample", fields)
        self.assertIn("local_start_sample", fields)
        self.assertIn("local_end_sample", fields)
