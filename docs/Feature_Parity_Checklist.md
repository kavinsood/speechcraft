# SpeechCraft vs Label Studio: Feature Parity Checklist

## Execution Order

1. Quick wins first (`1-2 sprints`) so annotators feel immediate speed gains.
2. Segment model next (`2-4 sprints`) to unlock Label Studio-style workflow.
3. Multichannel plus production media pipeline last (`platform-level`) because both reshape core architecture.

## Feature Parity Matrix

| Bucket | Parity Target | Backend Tasks | Frontend Tasks | Effort |
| --- | --- | --- | --- | --- |
| Quick wins | Functional `Settings` panel (currently placeholder) | Add `GET/PATCH /api/projects/{id}/settings` in `backend/app/main.py`; define settings model in `backend/app/models.py`; persist settings in `backend/app/repository.py` | Wire settings button in `frontend/src/App.tsx` into a modal or drawer; add toggles for `auto-scroll`, `nudge step`, `jump step`, and `waveform height` | S |
| Quick wins | Expand keyboard shortcuts plus shortcut help modal | Optional: persist per-project shortcut overrides | Add a shortcut registry in `frontend/src/workspace/EditorPane.tsx`; add actions for skip, jump, split, delete, and next clip; add shortcut reference modal | S |
| Quick wins | Skip and Submit task flow parity | Add lightweight task transition endpoint (`skip current`, `submit and next`) or map to existing status endpoints in `backend/app/main.py` | Add footer controls in `frontend/src/pages/LabelPage.tsx`; preserve current `Save Clip & Next` and `Reject Clip & Next` fast path | S |
| Quick wins | Better transport controls (backward, forward, jump) | No API changes required beyond existing audio endpoints | Add explicit transport buttons plus numeric jump controls in `frontend/src/workspace/EditorPane.tsx` | S |
| Medium | Segment entity (single-channel first) | Add `Segment` model plus CRUD endpoints (`/clips/{id}/segments`); snapshot segment state in clip commits in `backend/app/repository.py` | Replace single selection region in `frontend/src/WaveformPane.tsx` with multi-segment blocks; add segment list/cards panel | M |
| Medium | Segment cards with speaker dropdown plus local segment play | Add segment fields (`speaker`, `text`, `start/end`, `labels`) plus ordering and validation | Add right sidebar segment cards with bi-directional sync (card click to timeline focus, timeline click to card focus) and local segment playback | M |
| Medium | Segment editing toolbar parity (`insert before/after`, `split`, `delete`, `label+`) | Add segment mutation endpoints in `backend/app/main.py`; persist changes in `backend/app/repository.py` | Add contextual floating segment toolbar and map actions to hotkeys | M |
| Medium | Snapping and nudge behavior | Add settings-backed snapping configuration and server-side boundary normalization | Implement snap-to-playhead, snap-to-segment-boundary, and nudge/jump behavior in timeline interactions in `frontend/src/WaveformPane.tsx` | M |
| Medium | Timeline minimap and overview | Optional endpoint for downsampled overview peaks per clip | Add minimap strip and viewport box navigation for long files | M |
| Platform-level | True multichannel lanes plus overlap swim-lanes | Extend media model for per-channel peaks and metadata in `backend/app/models.py`; add channel-aware segment storage | Redesign timeline to stacked channel lanes and overlap-aware segment layout | L |
| Platform-level | Real source-backed rendering pipeline (replace synthetic media path) | Replace synthetic media path in `backend/app/repository.py` with source-backed FFmpeg render plus peak generation jobs and retries | Add render/job states, retries, and media relink UX in editor and inspector panes | L |
| Platform-level | Reviewer QA workflows (assignment, review pass, disagreement resolution) | Add review task entities, assignment, and audit-trail APIs | Add reviewer mode, approve/rework queues, and QA metrics panels | L |

## Definition of Done by Bucket

1. Quick wins done: settings are functional, shortcuts are discoverable, and skip/submit controls exist.
2. Medium done: users annotate through segments/cards (not clip-only selection), with snapping and local segment playback.
3. Platform-level done: multichannel plus overlap workflows are first-class, and exports run through real source-backed media processing.

## Recommended Immediate Build Slice

1. Settings API plus settings panel
2. Shortcut registry plus shortcut modal
3. Segment data model plus read-only segment rendering
4. Segment CRUD plus segment cards
5. Segment toolbar plus snapping
