from __future__ import annotations

import json
import math
import sqlite3
import wave
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


def _json_load(raw: Any, fallback: Any) -> Any:
    if raw is None:
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return fallback
    return fallback


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def _duration_seconds(sample_rate: Any, num_samples: Any) -> float:
    try:
        rate = float(sample_rate)
        samples = float(num_samples)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(rate) or rate <= 0 or not math.isfinite(samples) or samples < 0:
        return 0.0
    return round(samples / rate, 6)


class NativeClipLabStore:
    def __init__(self, db_path: Path, media_root: Path) -> None:
        self.db_path = db_path
        self.media_root = media_root

    @contextmanager
    def _connect(self) -> Any:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _source_recording_view(self, connection: sqlite3.Connection, recording_id: str) -> dict[str, Any]:
        row = connection.execute(
            """
            select id, batch_id, parent_recording_id, sample_rate, num_channels, num_samples, processing_recipe
            from sourcerecording
            where id = ?
            """,
            (recording_id,),
        ).fetchone()
        if row is None:
            raise KeyError(recording_id)
        return {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "parent_recording_id": row["parent_recording_id"],
            "sample_rate": row["sample_rate"],
            "num_channels": row["num_channels"],
            "num_samples": row["num_samples"],
            "processing_recipe": row["processing_recipe"],
        }

    def _slice_row(self, connection: sqlite3.Connection, slice_id: str) -> sqlite3.Row:
        row = connection.execute("select * from slice where id = ?", (slice_id,)).fetchone()
        if row is None:
            raise KeyError(slice_id)
        return row

    def _transcript_row(self, connection: sqlite3.Connection, slice_id: str) -> sqlite3.Row | None:
        return connection.execute("select * from transcript where slice_id = ?", (slice_id,)).fetchone()

    def _variant_rows(self, connection: sqlite3.Connection, slice_id: str) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                select id, slice_id, is_original, generator_model, sample_rate, num_samples, file_path
                from audiovariant
                where slice_id = ?
                order by is_original desc, id asc
                """,
                (slice_id,),
            ).fetchall()
        )

    def _commit_rows(self, connection: sqlite3.Connection, slice_id: str) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                select id, slice_id, parent_commit_id, edl_operations, transcript_text, status,
                       tags_payload, active_variant_id_snapshot, message, is_milestone, created_at
                from editcommit
                where slice_id = ?
                order by created_at desc, id desc
                """,
                (slice_id,),
            ).fetchall()
        )

    def _active_commit_row(self, connection: sqlite3.Connection, active_commit_id: str | None) -> sqlite3.Row | None:
        if not active_commit_id:
            return None
        return connection.execute("select * from editcommit where id = ?", (active_commit_id,)).fetchone()

    def _tag_rows(self, connection: sqlite3.Connection, slice_id: str) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                select tag.id, tag.name, tag.color
                from tag
                join slicetaglink on slicetaglink.tag_id = tag.id
                where slicetaglink.slice_id = ?
                order by lower(tag.name), tag.id
                """,
                (slice_id,),
            ).fetchall()
        )

    def _variant_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "is_original": _to_bool(row["is_original"]),
            "generator_model": row["generator_model"],
            "sample_rate": row["sample_rate"],
            "num_samples": row["num_samples"],
        }

    def _tag_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "name": row["name"],
            "color": row["color"],
        }

    def _commit_view(self, row: sqlite3.Row) -> dict[str, Any]:
        tags_payload = _json_load(row["tags_payload"], [])
        return {
            "id": row["id"],
            "parent_commit_id": row["parent_commit_id"],
            "edl_operations": _json_load(row["edl_operations"], []),
            "transcript_text": row["transcript_text"] or "",
            "status": row["status"],
            "tags": [
                {
                    "id": f"{row['id']}-tag-{index}",
                    "name": str(tag.get("name", "")),
                    "color": str(tag.get("color", "#FFFFFF")),
                }
                for index, tag in enumerate(tags_payload)
                if isinstance(tag, dict) and str(tag.get("name", "")).strip()
            ],
            "active_variant_id": row["active_variant_id_snapshot"],
            "message": row["message"],
            "is_milestone": _to_bool(row["is_milestone"]),
            "created_at": row["created_at"],
        }

    def _transcript_summary_view(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "slice_id": row["slice_id"],
            "original_text": row["original_text"],
            "modified_text": row["modified_text"],
            "is_modified": _to_bool(row["is_modified"]),
        }

    def _transcript_view(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "id": row["id"],
            "original_text": row["original_text"],
            "modified_text": row["modified_text"],
            "is_modified": _to_bool(row["is_modified"]),
            "alignment_data": _json_load(row["alignment_data"], None),
        }

    def _can_redo(self, connection: sqlite3.Connection, slice_id: str, active_commit_id: str | None) -> bool:
        if not active_commit_id:
            return False
        row = connection.execute(
            "select 1 from editcommit where slice_id = ? and parent_commit_id = ? limit 1",
            (slice_id, active_commit_id),
        ).fetchone()
        return row is not None

    def _slice_summary_from_row(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        metadata = _json_load(row["model_metadata"], {})
        transcript_row = self._transcript_row(connection, row["id"])
        tag_rows = self._tag_rows(connection, row["id"])
        variant_rows = self._variant_rows(connection, row["id"])
        active_variant = next((variant for variant in variant_rows if variant["id"] == row["active_variant_id"]), None)
        active_commit = self._active_commit_row(connection, row["active_commit_id"])
        return {
            "id": row["id"],
            "source_recording_id": row["source_recording_id"],
            "active_variant_id": row["active_variant_id"],
            "active_commit_id": row["active_commit_id"],
            "status": row["status"],
            "is_locked": _to_bool(row["is_locked"]),
            "duration_seconds": _duration_seconds(
                active_variant["sample_rate"] if active_variant else 0,
                active_variant["num_samples"] if active_variant else 0,
            ),
            "model_metadata": metadata,
            "created_at": row["created_at"],
            "transcript": self._transcript_summary_view(transcript_row),
            "tags": [self._tag_view(tag_row) for tag_row in tag_rows],
            "active_variant_generator_model": active_variant["generator_model"] if active_variant else None,
            "can_undo": active_commit is not None and active_commit["parent_commit_id"] is not None,
            "can_redo": self._can_redo(connection, row["id"], row["active_commit_id"]),
        }

    def list_project_slices(self, project_id: str) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                select slice.*
                from slice
                join sourcerecording on sourcerecording.id = slice.source_recording_id
                where sourcerecording.batch_id = ?
                order by sourcerecording.id asc, slice.created_at asc, slice.id asc
                """,
                (project_id,),
            ).fetchall()
            if not rows:
                project = connection.execute("select id from importbatch where id = ?", (project_id,)).fetchone()
                if project is None:
                    raise KeyError(project_id)
                return []
            summaries: list[dict[str, Any]] = []
            for row in rows:
                metadata = _json_load(row["model_metadata"], {})
                if metadata.get("is_superseded"):
                    continue
                summaries.append(self._slice_summary_from_row(connection, row))
            summaries.sort(
                key=lambda item: (
                    item["source_recording_id"],
                    int((item.get("model_metadata") or {}).get("order_index", 0)),
                    item["created_at"],
                    item["id"],
                )
            )
            return summaries

    def get_clip_lab_item(self, slice_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            slice_row = self._slice_row(connection, slice_id)
            metadata = _json_load(slice_row["model_metadata"], {})
            transcript_row = self._transcript_row(connection, slice_id)
            tag_rows = self._tag_rows(connection, slice_id)
            variant_rows = self._variant_rows(connection, slice_id)
            commit_rows = self._commit_rows(connection, slice_id)
            active_variant = next((variant for variant in variant_rows if variant["id"] == slice_row["active_variant_id"]), None)
            active_commit = next((commit for commit in commit_rows if commit["id"] == slice_row["active_commit_id"]), None)
            start_seconds = float(metadata.get("original_start_time", 0.0) or 0.0)
            duration_seconds = _duration_seconds(
                active_variant["sample_rate"] if active_variant else 0,
                active_variant["num_samples"] if active_variant else 0,
            )
            end_seconds = float(metadata.get("original_end_time", start_seconds + duration_seconds) or (start_seconds + duration_seconds))
            item = {
                "id": slice_row["id"],
                "kind": "slice",
                "source_recording_id": slice_row["source_recording_id"],
                "source_recording": self._source_recording_view(connection, slice_row["source_recording_id"]),
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "duration_seconds": duration_seconds,
                "status": slice_row["status"],
                "is_locked": _to_bool(slice_row["is_locked"]),
                "created_at": slice_row["created_at"],
                "transcript": self._transcript_view(transcript_row),
                "tags": [self._tag_view(tag_row) for tag_row in tag_rows],
                "speaker_name": metadata.get("speaker_name"),
                "language": metadata.get("language"),
                "audio_url": f"/media/variants/{slice_row['active_variant_id']}.wav" if slice_row["active_variant_id"] else "",
                "item_metadata": metadata,
                "transcript_source": None,
                "can_run_asr": False,
                "asr_placeholder_message": None,
                "active_variant_generator_model": active_variant["generator_model"] if active_variant else None,
                "can_undo": active_commit is not None and active_commit["parent_commit_id"] is not None,
                "can_redo": self._can_redo(connection, slice_id, slice_row["active_commit_id"]),
                "capabilities": {
                    "can_edit_transcript": True,
                    "can_edit_tags": True,
                    "can_set_status": True,
                    "can_save": True,
                    "can_split": False,
                    "can_merge": False,
                    "can_edit_waveform": False,
                    "can_run_processing": False,
                    "can_switch_variants": False,
                    "can_export": False,
                    "can_finalize": False,
                },
                "variants": [self._variant_view(row) for row in variant_rows],
                "commits": [self._commit_view(row) for row in commit_rows],
                "active_variant": self._variant_view(active_variant) if active_variant else None,
                "active_commit": self._commit_view(active_commit) if active_commit else None,
            }
            return item

    def get_variant_media_path(self, variant_id: str) -> Path:
        with self._connect() as connection:
            row = connection.execute("select file_path from audiovariant where id = ?", (variant_id,)).fetchone()
            if row is None:
                raise KeyError(variant_id)
            path = Path(row["file_path"]).expanduser()
            if not path.is_file():
                raise FileNotFoundError(path)
            return path

    def get_waveform_peaks(self, slice_id: str, bins: int = 120) -> dict[str, Any]:
        safe_bins = max(32, min(int(bins), 2048))
        path = self.get_variant_media_path(self._active_variant_id(slice_id))
        with wave.open(str(path), "rb") as handle:
            frames = handle.readframes(handle.getnframes())
            channels = max(handle.getnchannels(), 1)
        samples = np.frombuffer(frames, dtype="<i2")
        if channels > 1 and len(samples) >= channels:
            samples = samples[: len(samples) - (len(samples) % channels)].reshape(-1, channels).mean(axis=1)
        if len(samples) == 0:
            peaks = [0.0] * safe_bins
        else:
            normalized = np.abs(samples.astype(np.float64) / 32767.0)
            chunks = np.array_split(normalized, safe_bins)
            peaks = [float(chunk.max()) if len(chunk) > 0 else 0.0 for chunk in chunks]
        return {
            "clip_id": slice_id,
            "bins": safe_bins,
            "peaks": peaks,
        }

    def _active_variant_id(self, slice_id: str) -> str:
        with self._connect() as connection:
            row = connection.execute("select active_variant_id from slice where id = ?", (slice_id,)).fetchone()
            if row is None or not row["active_variant_id"]:
                raise KeyError(slice_id)
            return str(row["active_variant_id"])

    def _current_tags(self, connection: sqlite3.Connection, slice_id: str) -> list[dict[str, str]]:
        return [
            {"name": row["name"], "color": row["color"]}
            for row in self._tag_rows(connection, slice_id)
        ]

    def _current_transcript_text(self, transcript_row: sqlite3.Row | None) -> str:
        if transcript_row is None:
            return ""
        modified = transcript_row["modified_text"]
        if modified is not None and str(modified).strip():
            return str(modified)
        return str(transcript_row["original_text"] or "")

    def _ensure_baseline_commit(self, connection: sqlite3.Connection, slice_row: sqlite3.Row, transcript_row: sqlite3.Row | None) -> str:
        active_commit_id = slice_row["active_commit_id"]
        if active_commit_id:
            return str(active_commit_id)
        commit_id = _new_id("commit")
        connection.execute(
            """
            insert into editcommit (
              id, slice_id, parent_commit_id, edl_operations, created_at,
              transcript_text, status, tags_payload, active_variant_id_snapshot, message, is_milestone
            ) values (?, ?, null, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                commit_id,
                slice_row["id"],
                json.dumps([]),
                _utc_now_iso(),
                self._current_transcript_text(transcript_row),
                slice_row["status"],
                json.dumps(self._current_tags(connection, slice_row["id"])),
                slice_row["active_variant_id"],
                "Baseline",
            ),
        )
        connection.execute("update slice set active_commit_id = ? where id = ?", (commit_id, slice_row["id"]))
        return commit_id

    def _replace_tags(self, connection: sqlite3.Connection, slice_id: str, tags: list[dict[str, str]]) -> None:
        connection.execute("delete from slicetaglink where slice_id = ?", (slice_id,))
        for tag in tags:
            name = str(tag.get("name", "")).strip()
            if not name:
                continue
            color = str(tag.get("color", "#FFFFFF") or "#FFFFFF")
            tag_row = connection.execute("select id from tag where lower(name) = lower(?)", (name,)).fetchone()
            tag_id = tag_row["id"] if tag_row else _new_id("tag")
            if tag_row is None:
                connection.execute(
                    "insert into tag (id, name, color) values (?, ?, ?)",
                    (tag_id, name, color),
                )
            else:
                connection.execute("update tag set color = ? where id = ?", (color, tag_id))
            connection.execute(
                "insert into slicetaglink (slice_id, tag_id) values (?, ?)",
                (slice_id, tag_id),
            )

    def save_slice_state(self, slice_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._connect() as connection:
            slice_row = self._slice_row(connection, slice_id)
            transcript_row = self._transcript_row(connection, slice_id)
            current_commit_id = self._ensure_baseline_commit(connection, slice_row, transcript_row)
            next_status = payload.get("status") or slice_row["status"]
            next_text = (
                payload["modified_text"]
                if "modified_text" in payload and payload.get("modified_text") is not None
                else self._current_transcript_text(transcript_row)
            )
            next_tags = (
                payload["tags"]
                if "tags" in payload and payload.get("tags") is not None
                else self._current_tags(connection, slice_id)
            )
            if transcript_row is None:
                connection.execute(
                    """
                    insert into transcript (id, slice_id, original_text, modified_text, is_modified, alignment_data)
                    values (?, ?, ?, ?, ?, null)
                    """,
                    (_new_id("transcript"), slice_id, next_text, next_text, 0),
                )
            else:
                original_text = str(transcript_row["original_text"] or "")
                connection.execute(
                    "update transcript set modified_text = ?, is_modified = ? where slice_id = ?",
                    (next_text, 1 if next_text != original_text else 0, slice_id),
                )
            self._replace_tags(connection, slice_id, next_tags)
            connection.execute("update slice set status = ? where id = ?", (next_status, slice_id))
            commit_id = _new_id("commit")
            connection.execute(
                """
                insert into editcommit (
                  id, slice_id, parent_commit_id, edl_operations, created_at,
                  transcript_text, status, tags_payload, active_variant_id_snapshot, message, is_milestone
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    commit_id,
                    slice_id,
                    current_commit_id,
                    self._active_commit_row(connection, current_commit_id)["edl_operations"] if self._active_commit_row(connection, current_commit_id) else json.dumps([]),
                    _utc_now_iso(),
                    next_text,
                    next_status,
                    json.dumps(next_tags),
                    slice_row["active_variant_id"],
                    payload.get("message"),
                    1 if _to_bool(payload.get("is_milestone")) else 0,
                ),
            )
            connection.execute("update slice set active_commit_id = ? where id = ?", (commit_id, slice_id))
            connection.commit()
        return self.get_slice_summary(slice_id)

    def get_slice_summary(self, slice_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            return self._slice_summary_from_row(connection, self._slice_row(connection, slice_id))

    def update_slice_status(self, slice_id: str, status: str) -> dict[str, Any]:
        return self.save_slice_state(
            slice_id,
            {
                "status": status,
                "message": f"Status: {str(status).replace('_', ' ')}",
            },
        )

    def update_slice_transcript(self, slice_id: str, modified_text: str) -> dict[str, Any]:
        return self.save_slice_state(slice_id, {"modified_text": modified_text, "message": "Transcript updated"})

    def update_slice_tags(self, slice_id: str, tags: list[dict[str, str]]) -> dict[str, Any]:
        return self.save_slice_state(slice_id, {"tags": tags, "message": "Tags updated"})

    def _restore_commit(self, connection: sqlite3.Connection, slice_id: str, target_commit: sqlite3.Row) -> None:
        connection.execute("update slice set status = ?, active_commit_id = ? where id = ?", (target_commit["status"], target_commit["id"], slice_id))
        transcript_row = self._transcript_row(connection, slice_id)
        if transcript_row is None:
            connection.execute(
                """
                insert into transcript (id, slice_id, original_text, modified_text, is_modified, alignment_data)
                values (?, ?, ?, ?, ?, null)
                """,
                (_new_id("transcript"), slice_id, target_commit["transcript_text"], target_commit["transcript_text"], 0),
            )
        else:
            original_text = str(transcript_row["original_text"] or "")
            text = str(target_commit["transcript_text"] or "")
            connection.execute(
                "update transcript set modified_text = ?, is_modified = ? where slice_id = ?",
                (text, 1 if text != original_text else 0, slice_id),
            )
        tags_payload = _json_load(target_commit["tags_payload"], [])
        self._replace_tags(connection, slice_id, [tag for tag in tags_payload if isinstance(tag, dict)])
        if target_commit["active_variant_id_snapshot"]:
            connection.execute(
                "update slice set active_variant_id = ? where id = ?",
                (target_commit["active_variant_id_snapshot"], slice_id),
            )

    def undo_slice(self, slice_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            slice_row = self._slice_row(connection, slice_id)
            active_commit = self._active_commit_row(connection, slice_row["active_commit_id"])
            if active_commit is None or active_commit["parent_commit_id"] is None:
                raise ValueError("No earlier edit state is available")
            target = self._active_commit_row(connection, active_commit["parent_commit_id"])
            if target is None:
                raise ValueError("No earlier edit state is available")
            self._restore_commit(connection, slice_id, target)
            connection.commit()
        return self.get_slice_summary(slice_id)

    def redo_slice(self, slice_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            slice_row = self._slice_row(connection, slice_id)
            target = connection.execute(
                """
                select *
                from editcommit
                where slice_id = ? and parent_commit_id = ?
                order by created_at desc, id desc
                limit 1
                """,
                (slice_id, slice_row["active_commit_id"]),
            ).fetchone()
            if target is None:
                raise ValueError("No newer edit state is available")
            self._restore_commit(connection, slice_id, target)
            connection.commit()
        return self.get_slice_summary(slice_id)
