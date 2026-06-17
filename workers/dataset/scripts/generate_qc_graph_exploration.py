#!/usr/bin/env python3
"""Generate QC metric exploration plots for mb / mc / poki."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DATASETS = ("mb", "mc", "poki")
SPEAKER_PURITY_THRESHOLD = 0.70
TRANSCRIPT_MATCH_THRESHOLD = 85.0

TORTURE_VARIANTS = [
    "splice_400ms_mid",
    "splice_1s_mid",
    "splice_2s_mid",
    "blend_25_2s_mid",
    "blend_50_2s_mid",
    "blend_75_2s_mid",
    "blend_50_4s_mid",
    "replace_2s_mid",
    "prepend_1s",
    "append_1s",
]

PLOT_SPECS: list[dict[str, Any]] = []


def log(message: str) -> None:
    print(f"[qc_graph_exploration] {message}", flush=True)


def p10(values: list[float]) -> float | None:
    if not values:
        return None
    return float(np.percentile(values, 10))


def load_clip_metrics(dataset: str) -> pd.DataFrame:
    purity_path = REPO / "backend/data/eval_speaker_purity" / dataset / "speaker_purity_qc.json"
    ctc_path = REPO / "backend/data/eval_ctc_qc_v2" / dataset / "ctc_transcript_qc.json"
    purity_rows = {row["clip_id"]: row for row in json.loads(purity_path.read_text())}
    ctc_rows = json.loads(ctc_path.read_text())

    records: list[dict[str, Any]] = []
    for ctc in ctc_rows:
        clip_id = str(ctc.get("clip_id") or "")
        purity = purity_rows.get(clip_id) or {}
        windows = purity.get("windows") or []
        window_sims = [float(w["similarity"]) for w in windows if w.get("similarity") is not None]
        records.append(
            {
                "dataset": dataset,
                "clip_id": clip_id,
                "duration_sec": ctc.get("duration_sec") or purity.get("duration_sec"),
                "audio_path": ctc.get("audio_path") or purity.get("audio_path"),
                "verifier_text": ctc.get("verifier_text"),
                "transcript_match_score": ctc.get("transcript_match_score"),
                "ctc_bucket": ctc.get("bucket"),
                "unaligned_speech_ratio": ctc.get("unaligned_speech_ratio"),
                "speaker_purity_score": purity.get("purity_score"),
                "min_window_similarity": purity.get("min_window_similarity") or purity.get("purity_score"),
                "mean_window_similarity": purity.get("mean_window_similarity"),
                "p10_window_similarity": p10(window_sims),
                "speaker_purity_bucket": purity.get("bucket"),
                "intruder_window_count": purity.get("intruder_window_count"),
                "scored_window_count": purity.get("scored_window_count"),
            }
        )
    return pd.DataFrame.from_records(records)


def save_plot(fig: plt.Figure, plots_dir: Path, tables_dir: Path, stem: str, table: pd.DataFrame | None = None) -> None:
    png = plots_dir / f"{stem}.png"
    fig.savefig(png, dpi=160, bbox_inches="tight")
    plt.close(fig)
    if table is not None:
        table.to_csv(tables_dir / f"{stem}.csv", index=False)
    PLOT_SPECS.append({"stem": stem, "png": str(png.relative_to(plots_dir.parent))})


def plot_histograms(df: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    metrics = [
        ("transcript_match_score", "01_hist_transcript_match_by_dataset", "Transcript match score"),
        ("speaker_purity_score", "02_hist_speaker_purity_by_dataset", "Speaker purity score"),
        ("min_window_similarity", "03_hist_min_window_similarity_by_dataset", "Min window similarity"),
    ]
    colors = {"mb": "#4C78A8", "mc": "#F58518", "poki": "#54A24B"}
    for metric, stem, title in metrics:
        subset = df.dropna(subset=[metric])
        bins = np.linspace(subset[metric].min(), subset[metric].max(), 30)
        fig, axes = plt.subplots(1, len(DATASETS), figsize=(14, 4.5), sharex=True, sharey=True)
        if len(DATASETS) == 1:
            axes = [axes]
        rows = []
        ymax = 0
        for ax, dataset in zip(axes, DATASETS, strict=True):
            values = subset.loc[subset["dataset"] == dataset, metric]
            counts, edges, _patches = ax.hist(values, bins=bins, color=colors[dataset], edgecolor="white", linewidth=0.6)
            ymax = max(ymax, float(max(counts)) if len(counts) else 0.0)
            ax.set_title(f"{dataset} (n={len(values)})")
            ax.set_xlabel(metric)
            rows.append(
                {
                    "dataset": dataset,
                    "metric": metric,
                    "count": len(values),
                    "p50": values.median(),
                    "p10": values.quantile(0.1),
                    "p90": values.quantile(0.9),
                }
            )
        for ax in axes:
            ax.set_ylim(0, ymax * 1.08 if ymax else 1)
        axes[0].set_ylabel("clip count")
        fig.suptitle(title, y=1.02)
        fig.tight_layout()
        save_plot(fig, plots_dir, tables_dir, stem, pd.DataFrame(rows))


def survival_curve(values: pd.Series, thresholds: np.ndarray) -> np.ndarray:
    arr = values.dropna().to_numpy(dtype=float)
    if arr.size == 0:
        return np.zeros_like(thresholds)
    return np.array([(arr >= t).mean() * 100.0 for t in thresholds], dtype=float)


def plot_threshold_survival(df: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    specs = [
        ("speaker_purity_score", np.linspace(0.45, 0.95, 101), "04_threshold_survival_speaker_purity", "Speaker purity threshold", SPEAKER_PURITY_THRESHOLD),
        ("transcript_match_score", np.linspace(60, 100, 81), "05_threshold_survival_transcript_match", "Transcript match threshold", TRANSCRIPT_MATCH_THRESHOLD),
    ]
    colors = {"mb": "#4C78A8", "mc": "#F58518", "poki": "#54A24B"}
    for metric, thresholds, stem, xlabel, ref in specs:
        fig, ax = plt.subplots(figsize=(10, 6))
        rows = []
        for dataset in DATASETS:
            values = df.loc[df["dataset"] == dataset, metric]
            kept = survival_curve(values, thresholds)
            ax.plot(thresholds, kept, label=dataset, color=colors[dataset], linewidth=2)
            ref_kept = float((values.dropna() >= ref).mean() * 100.0)
            rows.append(
                {
                    "dataset": dataset,
                    "metric": metric,
                    "reference_threshold": ref,
                    "pct_kept_at_reference": round(ref_kept, 2),
                }
            )
        ax.axvline(ref, color="#666666", linestyle="--", linewidth=1, alpha=0.8)
        ax.set_title(f"Clips kept vs {metric}")
        ax.set_xlabel(xlabel)
        ax.set_ylabel("% clips kept")
        ax.set_ylim(0, 101)
        ax.legend()
        save_plot(fig, plots_dir, tables_dir, stem, pd.DataFrame(rows))


def plot_accepted_duration(df: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    thresholds = np.linspace(0.45, 0.95, 101)
    transcript_thresholds = np.linspace(60, 100, 81)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    colors = {"mb": "#4C78A8", "mc": "#F58518", "poki": "#54A24B"}
    rows: list[dict[str, Any]] = []

    for dataset in DATASETS:
        sub = df.loc[df["dataset"] == dataset].dropna(subset=["speaker_purity_score", "duration_sec"])
        total_min = sub["duration_sec"].sum() / 60.0
        kept_min = [sub.loc[sub["speaker_purity_score"] >= t, "duration_sec"].sum() / 60.0 for t in thresholds]
        axes[0].plot(thresholds, kept_min, label=f"{dataset} ({total_min:.1f} min total)", color=colors[dataset])
        rows.append(
            {
                "dataset": dataset,
                "metric": "speaker_purity_score",
                "total_duration_min": round(total_min, 3),
                "threshold": SPEAKER_PURITY_THRESHOLD,
                "duration_kept_min_at_ref": round(sub.loc[sub["speaker_purity_score"] >= SPEAKER_PURITY_THRESHOLD, "duration_sec"].sum() / 60.0, 3),
            }
        )

    axes[0].axvline(SPEAKER_PURITY_THRESHOLD, color="#666666", linestyle="--", linewidth=1)
    axes[0].set_title("Accepted duration vs speaker purity threshold")
    axes[0].set_xlabel("speaker_purity_score threshold")
    axes[0].set_ylabel("accepted minutes")
    axes[0].legend(fontsize=8)

    for dataset in DATASETS:
        sub = df.loc[df["dataset"] == dataset].dropna(subset=["transcript_match_score", "duration_sec"])
        total_min = sub["duration_sec"].sum() / 60.0
        kept_min = [sub.loc[sub["transcript_match_score"] >= t, "duration_sec"].sum() / 60.0 for t in transcript_thresholds]
        axes[1].plot(transcript_thresholds, kept_min, label=f"{dataset} ({total_min:.1f} min total)", color=colors[dataset])
        rows.append(
            {
                "dataset": dataset,
                "metric": "transcript_match_score",
                "total_duration_min": round(total_min, 3),
                "threshold": TRANSCRIPT_MATCH_THRESHOLD,
                "duration_kept_min_at_ref": round(sub.loc[sub["transcript_match_score"] >= TRANSCRIPT_MATCH_THRESHOLD, "duration_sec"].sum() / 60.0, 3),
            }
        )

    axes[1].axvline(TRANSCRIPT_MATCH_THRESHOLD, color="#666666", linestyle="--", linewidth=1)
    axes[1].set_title("Accepted duration vs transcript match threshold")
    axes[1].set_xlabel("transcript_match_score threshold")
    axes[1].set_ylabel("accepted minutes")
    axes[1].legend(fontsize=8)
    fig.tight_layout()
    save_plot(fig, plots_dir, tables_dir, "06_accepted_duration_vs_threshold", pd.DataFrame(rows))


def plot_scatter(
    df: pd.DataFrame,
    x: str,
    y: str,
    stem: str,
    title: str,
    plots_dir: Path,
    tables_dir: Path,
    size_col: str | None = None,
) -> None:
    subset = df.dropna(subset=[x, y]).copy()
    colors = {"mb": "#4C78A8", "mc": "#F58518", "poki": "#54A24B"}
    fig, ax = plt.subplots(figsize=(10, 7))
    for dataset in DATASETS:
        part = subset.loc[subset["dataset"] == dataset]
        sizes = None
        if size_col:
            sizes = 20 + 120 * (part[size_col] / max(subset[size_col].max(), 1e-6))
        ax.scatter(part[x], part[y], s=sizes, alpha=0.65, label=dataset, color=colors[dataset], edgecolors="none")
    ax.set_title(title)
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.legend()
    export_cols = [
        "dataset",
        "clip_id",
        x,
        y,
        "duration_sec",
        "audio_path",
        "verifier_text",
        "ctc_bucket",
        "speaker_purity_bucket",
    ]
    save_plot(fig, plots_dir, tables_dir, stem, subset[export_cols])


def plot_duration_scatters(df: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    plot_scatter(df, "duration_sec", "speaker_purity_score", "09_scatter_speaker_purity_vs_duration", "Speaker purity vs clip duration", plots_dir, tables_dir)
    plot_scatter(df, "duration_sec", "transcript_match_score", "10_scatter_transcript_match_vs_duration", "Transcript match vs clip duration", plots_dir, tables_dir)


def plot_elbows(df: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    specs = [
        ("speaker_purity_score", "15_elbow_speaker_purity_by_dataset", "Sorted speaker purity scores"),
        ("transcript_match_score", "16_elbow_transcript_match_by_dataset", "Sorted transcript match scores"),
    ]
    colors = {"mb": "#4C78A8", "mc": "#F58518", "poki": "#54A24B"}
    for metric, stem, title in specs:
        fig, ax = plt.subplots(figsize=(10, 6))
        rows = []
        for dataset in DATASETS:
            values = df.loc[df["dataset"] == dataset, metric].dropna().sort_values().to_numpy()
            if values.size == 0:
                continue
            ax.plot(np.arange(1, values.size + 1), values, label=dataset, color=colors[dataset], linewidth=1.5)
            rows.append(
                {
                    "dataset": dataset,
                    "metric": metric,
                    "worst_score": values[0],
                    "p10_rank_score": values[max(0, int(0.1 * values.size) - 1)],
                    "median_score": np.median(values),
                }
            )
        ax.set_title(title)
        ax.set_xlabel("sorted clip rank (worst → best)")
        ax.set_ylabel(metric)
        ax.legend()
        save_plot(fig, plots_dir, tables_dir, stem, pd.DataFrame(rows))


def load_torture_trials() -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for dataset in ("mb", "mc"):
        path = REPO / "backend/data/eval_speaker_purity_battery" / dataset / "speaker_purity_battery_trials.csv"
        if path.exists():
            part = pd.read_csv(path)
            part["dataset"] = dataset
            rows.append(part)
    if not rows:
        return pd.DataFrame()
    trials = pd.concat(rows, ignore_index=True)
    trials = trials.loc[~trials["variant"].isin(["natural_worst_rescore"])].copy()
    trials["caught"] = trials["purity_score"] < SPEAKER_PURITY_THRESHOLD
    return trials


def plot_torture_battery(trials: pd.DataFrame, plots_dir: Path, tables_dir: Path) -> None:
    if trials.empty:
        log("no torture battery trials found; skipping plots 17-18")
        return

    variants = [v for v in TORTURE_VARIANTS if v in set(trials["variant"])]
    grouped = (
        trials.loc[trials["variant"].isin(variants)]
        .groupby(["dataset", "variant"], as_index=False)
        .agg(detection_rate=("caught", "mean"), mean_score_delta=("score_delta", "mean"), trial_count=("caught", "size"))
    )
    grouped["detection_pct"] = grouped["detection_rate"] * 100.0

    x = np.arange(len(variants))
    width = 0.35
    fig, ax = plt.subplots(figsize=(14, 6))
    for dataset in ("mb", "mc"):
        part = grouped.loc[grouped["dataset"] == dataset].set_index("variant").reindex(variants)
        offset = -width / 2 if dataset == "mb" else width / 2
        ax.bar(x + offset, part["detection_pct"], width=width, label=dataset)
    ax.set_xticks(x)
    ax.set_xticklabels(variants, rotation=45, ha="right")
    ax.set_ylabel("% caught below 0.70")
    ax.set_title("Speaker purity torture detection rate by variant")
    ax.legend()
    fig.tight_layout()
    save_plot(fig, plots_dir, tables_dir, "17_torture_detection_rate_by_variant", grouped)

    fig, ax = plt.subplots(figsize=(14, 6))
    for dataset in ("mb", "mc"):
        part = grouped.loc[grouped["dataset"] == dataset].set_index("variant").reindex(variants)
        offset = -width / 2 if dataset == "mb" else width / 2
        ax.bar(x + offset, part["mean_score_delta"], width=width, label=dataset)
    ax.set_xticks(x)
    ax.set_xticklabels(variants, rotation=45, ha="right")
    ax.set_ylabel("mean baseline − contaminated score")
    ax.set_title("Speaker purity torture score delta by variant")
    ax.legend()
    fig.tight_layout()
    save_plot(fig, plots_dir, tables_dir, "18_torture_score_delta_by_variant", grouped)


def write_summary(out_root: Path, df: pd.DataFrame, trials: pd.DataFrame) -> None:
    lines = [
        "# QC graph exploration summary",
        "",
        "Exploratory plots for mb / mc / poki candidate review clips.",
        "Not a QC page design — metric landscape only.",
        "",
        "## Clip counts",
        "",
    ]
    for dataset in DATASETS:
        sub = df.loc[df["dataset"] == dataset]
        lines.append(f"- **{dataset}**: {len(sub)} clips, {sub['duration_sec'].sum() / 60:.1f} min total")
    lines.extend(
        [
            "",
            "## Reference thresholds",
            "",
            f"- speaker_purity_score ≥ {SPEAKER_PURITY_THRESHOLD}",
            f"- transcript_match_score ≥ {TRANSCRIPT_MATCH_THRESHOLD}",
            "",
        ]
    )

    for metric, ref in [("speaker_purity_score", SPEAKER_PURITY_THRESHOLD), ("transcript_match_score", TRANSCRIPT_MATCH_THRESHOLD)]:
        lines.append(f"### {metric} @ {ref}")
        for dataset in DATASETS:
            sub = df.loc[df["dataset"] == dataset, metric].dropna()
            kept = (sub >= ref).mean() * 100.0
            lines.append(f"- {dataset}: {kept:.1f}% clips kept")
        lines.append("")

    if not trials.empty:
        lines.append("## Torture battery (mb vs mc)")
        for dataset in ("mb", "mc"):
            sub = trials.loc[trials["dataset"] == dataset]
            lines.append(f"- {dataset}: {sub['caught'].mean() * 100:.1f}% trials caught below 0.70 overall")
        lines.append("")

    lines.extend(["## Plots generated", ""])
    for spec in PLOT_SPECS:
        lines.append(f"- `{spec['png']}`")
    (out_root / "graph_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_index(out_root: Path) -> None:
    sections = []
    for spec in PLOT_SPECS:
        png = html.escape(spec["png"])
        stem = html.escape(spec["stem"])
        table = html.escape(f"tables/{spec['stem']}.csv")
        sections.append(
            f"<section><h2>{stem}</h2><p><a href=\"{table}\">source csv</a></p><img src=\"{png}\" alt=\"{stem}\" /></section>"
        )
    body = "\n".join(sections)
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>QC graph exploration</title>
  <style>
    body {{ font-family: sans-serif; max-width: 1100px; margin: 2rem auto; line-height: 1.5; }}
    img {{ max-width: 100%; border: 1px solid #ddd; margin: 1rem 0; }}
    section {{ margin-bottom: 2.5rem; }}
  </style>
</head>
<body>
  <h1>QC graph exploration pack</h1>
  <p>First 8 plot groups for mb / mc / poki. See <a href="graph_summary.md">graph_summary.md</a>.</p>
  {body}
</body>
</html>
"""
    (out_root / "graph_index.html").write_text(content, encoding="utf-8")


def generate(out_root: Path) -> None:
    plots_dir = out_root / "plots"
    tables_dir = out_root / "tables"
    for path in (plots_dir, tables_dir, *(out_root / ds for ds in DATASETS), out_root / "combined"):
        path.mkdir(parents=True, exist_ok=True)

    frames = [load_clip_metrics(dataset) for dataset in DATASETS]
    df = pd.concat(frames, ignore_index=True)
    for dataset, frame in zip(DATASETS, frames, strict=True):
        frame.to_csv(out_root / dataset / "clip_metrics.csv", index=False)
    df.to_csv(out_root / "combined" / "clip_metrics.csv", index=False)

    plot_histograms(df, plots_dir, tables_dir)
    plot_threshold_survival(df, plots_dir, tables_dir)
    plot_accepted_duration(df, plots_dir, tables_dir)
    plot_scatter(
        df,
        "transcript_match_score",
        "speaker_purity_score",
        "07_scatter_transcript_vs_speaker_purity",
        "Transcript match vs speaker purity",
        plots_dir,
        tables_dir,
        size_col="duration_sec",
    )
    plot_scatter(
        df,
        "mean_window_similarity",
        "min_window_similarity",
        "08_scatter_speaker_min_vs_mean",
        "Speaker min vs mean window similarity",
        plots_dir,
        tables_dir,
    )
    plot_duration_scatters(df, plots_dir, tables_dir)
    plot_elbows(df, plots_dir, tables_dir)

    trials = load_torture_trials()
    plot_torture_battery(trials, plots_dir, tables_dir)

    write_summary(out_root, df, trials)
    write_index(out_root)
    log(f"wrote {len(PLOT_SPECS)} plots to {out_root}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate QC metric exploration plots.")
    parser.add_argument(
        "--out",
        default=str(REPO / "backend/data/qc_graph_exploration"),
        help="Output root directory",
    )
    args = parser.parse_args()
    generate(Path(args.out).expanduser().resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
