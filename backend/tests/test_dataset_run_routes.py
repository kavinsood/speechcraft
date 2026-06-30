from __future__ import annotations

from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient
from fastapi import HTTPException

from app.main import (
    app,
    create_project_dataset_run,
    finalize_dataset_qc_route,
    generate_dataset_qc_scores_route,
    list_project_dataset_runs,
    read_dataset_export_results,
    read_dataset_qc,
    read_dataset_run,
    read_dataset_run_log,
    read_dataset_slicer_results,
    read_dataset_speakers,
    rerun_project_dataset_export,
    rerun_project_dataset_slicer,
    resume_project_dataset_run,
    system_preflight,
    update_dataset_speaker_selection,
)
from app.models import (
    DatasetExportResultsView,
    DatasetExportRerunRequest,
    DatasetQcFinalizeRequest,
    DatasetQcFinalizeResponse,
    DatasetQcFinalizeSummaryView,
    DatasetQcGenerateRequest,
    DatasetQcPayloadView,
    DatasetQcThresholdsRequest,
    DatasetRunArtifactView,
    DatasetRunCreateRequest,
    DatasetRunLogView,
    DatasetRunResumeRequest,
    DatasetRunView,
    DatasetSlicerResultsView,
    DatasetSlicerRerunRequest,
    DatasetSpeakerResultsView,
    DatasetSpeakerSelectionUpdateRequest,
    DatasetSpeakerSelectionView,
    ProcessingRunStatus,
    RfcStage,
    RunArtifactKind,
)


def run_view() -> DatasetRunView:
    return DatasetRunView(
        id="dataset-run-1",
        project_id="project-1",
        pipeline_version="pretraining_rfc_v1",
        artifact_root="dataset-runs/project-1/dataset-run-1",
        stage=RfcStage.INGEST,
        status=ProcessingRunStatus.PENDING,
        created_at=datetime(2026, 6, 5, tzinfo=timezone.utc),
        artifacts=[
            DatasetRunArtifactView(
                id="artifact-1",
                kind=RunArtifactKind.RUN_CONFIG_JSON,
                path="config.json",
            )
        ],
    )


class DatasetRunRouteTests(TestCase):
    def test_system_preflight_passes_selected_asr_model(self) -> None:
        with patch("app.main.run_dataset_worker_preflight", return_value={"ok": True}) as preflight:
            result = system_preflight(asr_model="medium.en", asr_device="cuda", asr_compute_type="float16")

        self.assertTrue(result["ok"])
        self.assertEqual(
            preflight.call_args.kwargs,
            {
                "artifact_root": None,
                "asr_model": "medium.en",
                "asr_model_path": None,
                "asr_cache_dir": None,
                "asr_device": "cuda",
                "asr_compute_type": "float16",
            },
        )

    def test_create_list_get_and_log_routes_serialize(self) -> None:
        run = run_view()
        payload = DatasetRunCreateRequest(source_recording_ids=["recording-1"], single_speaker=True)
        with (
            patch("app.main.create_dataset_run", return_value=run) as create,
            patch("app.main.list_dataset_runs", return_value=[run]),
            patch("app.main.get_dataset_run", return_value=run),
            patch(
                "app.main.get_dataset_run_log",
                return_value=DatasetRunLogView(
                    run_id=run.id,
                    path="logs/dataset_worker.log",
                    text="worker output",
                ),
            ),
        ):
            created = create_project_dataset_run("project-1", payload)
            listed = list_project_dataset_runs("project-1")
            fetched = read_dataset_run(run.id)
            log = read_dataset_run_log(run.id)

        self.assertEqual(created.id, run.id)
        self.assertEqual(listed[0].artifacts[0].kind, RunArtifactKind.RUN_CONFIG_JSON)
        self.assertEqual(fetched.id, run.id)
        self.assertEqual(log.text, "worker output")
        self.assertTrue(create.call_args.args[2].single_speaker)

    def test_create_route_surfaces_multi_speaker_validation_errors(self) -> None:
        with patch(
            "app.main.create_dataset_run",
            side_effect=ValueError(
                "Multi-speaker diarization currently supports exactly one source WAV per run"
            ),
        ):
            with self.assertRaises(HTTPException) as exc:
                create_project_dataset_run(
                    "project-1",
                    DatasetRunCreateRequest(single_speaker=False),
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("exactly one source WAV", str(exc.exception.detail))

    def test_slicer_rerun_and_results_routes_serialize(self) -> None:
        run = run_view()
        results = DatasetSlicerResultsView(
            run_id=run.id,
            safe_cutpoint_summary={"accepted_cutpoints": 3},
            candidate_review_summary={"candidate_review_clips": 2},
            candidate_review_manifest=[{"id": "clip-1"}],
        )
        with (
            patch("app.main.rerun_dataset_slicer", return_value=run) as rerun,
            patch("app.main.get_dataset_slicer_results", return_value=results),
        ):
            rerun_response = rerun_project_dataset_slicer(
                run.id,
                DatasetSlicerRerunRequest(config={"cutpoint_min_gap_ms": 40}),
            )
            results_response = read_dataset_slicer_results(run.id)

        self.assertEqual(rerun_response.id, run.id)
        self.assertEqual(results_response.safe_cutpoint_summary["accepted_cutpoints"], 3)
        self.assertEqual(rerun.call_args.args[2].config["cutpoint_min_gap_ms"], 40)

    def test_export_rerun_and_results_routes_serialize(self) -> None:
        run = run_view()
        results = DatasetExportResultsView(
            run_id=run.id,
            export_summary={"exported_clip_count": 2},
            export_manifest=[{"id": "clip-1"}],
            export_audit=[],
        )
        with (
            patch("app.main.rerun_dataset_native_export", return_value=run) as rerun,
            patch("app.main.get_dataset_export_results", return_value=results),
        ):
            rerun_response = rerun_project_dataset_export(run.id, DatasetExportRerunRequest())
            results_response = read_dataset_export_results(run.id)

        self.assertEqual(rerun_response.id, run.id)
        self.assertEqual(results_response.export_summary["exported_clip_count"], 2)
        self.assertEqual(rerun.call_args.args[2].config, {})

    def test_speaker_routes_serialize(self) -> None:
        run = run_view()
        results = DatasetSpeakerResultsView(
            run_id=run.id,
            speaker_regions_summary={"speaker_count": 2},
            speaker_samples_manifest=[],
            speaker_selection=DatasetSpeakerSelectionView(
                mode="diarization",
                selected=False,
                target_speaker_id=None,
                source="pending_user_selection",
                available_speaker_ids=["speaker_0", "speaker_1"],
            ),
        )
        selection = DatasetSpeakerSelectionView(
            mode="diarization",
            selected=True,
            target_speaker_id="speaker_0",
            source="user",
            available_speaker_ids=["speaker_0", "speaker_1"],
            updated_at="2026-06-05T00:00:00+00:00",
        )
        with (
            patch("app.main.get_dataset_speaker_results", return_value=results),
            patch("app.main.save_dataset_speaker_selection", return_value=selection),
            patch("app.main.resume_dataset_run_processing", return_value=run),
        ):
            results_response = read_dataset_speakers(run.id)
            selection_response = update_dataset_speaker_selection(
                run.id,
                DatasetSpeakerSelectionUpdateRequest(target_speaker_id="speaker_0"),
            )
            resume_response = resume_project_dataset_run(
                run.id,
                DatasetRunResumeRequest(stop_after="alignment_qc"),
            )

        self.assertEqual(results_response.speaker_regions_summary["speaker_count"], 2)
        self.assertEqual(selection_response.target_speaker_id, "speaker_0")
        self.assertEqual(resume_response.id, run.id)

    def test_qc_routes_serialize(self) -> None:
        run = run_view()
        payload = DatasetQcPayloadView(run_id=run.id, ready=True)
        finalize_response = DatasetQcFinalizeResponse(
            run_id=run.id,
            dataset_qc_path="artifacts/dataset_qc.json",
            summary=DatasetQcFinalizeSummaryView(
                accepted_count=2,
                rejected_count=1,
                accepted_duration_sec=12.5,
                rejected_duration_sec=3.2,
            ),
        )
        with (
            patch("app.main.get_dataset_qc", return_value=payload) as get_qc,
            patch("app.main.finalize_dataset_qc", return_value=finalize_response) as finalize,
        ):
            qc_payload = read_dataset_qc(run.id)
            finalize_result = finalize_dataset_qc_route(
                run.id,
                DatasetQcFinalizeRequest(
                    thresholds=DatasetQcThresholdsRequest(transcript_match_min=85, speaker_check_min=70),
                ),
            )

        self.assertTrue(qc_payload.ready)
        self.assertEqual(finalize_result.summary.accepted_count, 2)
        get_qc.assert_called_once()
        finalize.assert_called_once()

    def test_qc_generate_route_serializes(self) -> None:
        run = run_view()
        with patch("app.main.generate_dataset_qc_scores", return_value=run) as generate:
            response = generate_dataset_qc_scores_route(run.id)

        self.assertEqual(response.id, run.id)
        self.assertEqual(generate.call_args.args[1], run.id)

    def test_qc_generate_route_passes_force_flag(self) -> None:
        run = run_view()
        with patch("app.main.generate_dataset_qc_scores", return_value=run) as generate:
            response = generate_dataset_qc_scores_route(run.id, DatasetQcGenerateRequest(force=True))

        self.assertEqual(response.id, run.id)
        self.assertTrue(generate.call_args.kwargs["force"])


class DatasetQcGenerateRouteHttpTests(TestCase):
    def test_qc_generate_accepts_post_without_body(self) -> None:
        run = run_view()
        client = TestClient(app)
        with patch("app.main.generate_dataset_qc_scores", return_value=run) as generate:
            response = client.post(f"/api/dataset-runs/{run.id}/qc/generate")

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["id"], run.id)
        self.assertFalse(generate.call_args.kwargs["force"])

    def test_qc_generate_accepts_explicit_json_body(self) -> None:
        run = run_view()
        client = TestClient(app)
        with patch("app.main.generate_dataset_qc_scores", return_value=run) as generate:
            response = client.post(
                f"/api/dataset-runs/{run.id}/qc/generate",
                json={"force": False},
            )

        self.assertEqual(response.status_code, 202)
        self.assertFalse(generate.call_args.kwargs["force"])
