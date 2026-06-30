## Dataset QC UI Notes

Date: 2026-06-18

### Threshold curves

The QC page renders threshold impact curves with a custom inline SVG component in `frontend/src/components/qc/QcThresholdImpactCard.tsx`.

Reason:
- The frontend workspace does not currently depend on `recharts`.
- The slider and chart need one shared coordinate system so the live threshold marker stays aligned with the slider thumb.
- The page only needs two monotonic threshold curves plus hover readouts, so the current implementation keeps the chart surface minimal and avoids introducing a new dependency during this phase.

Constraint:
- This is an intentional v1 implementation decision, not an accidental fallback.
- Old runs that already have candidate clips but are missing transcript or speaker QC artifacts expose a `Run QC Scores` action on the QC page.
- Threshold curves autoscale vertically and use hover for exact threshold and duration values.
