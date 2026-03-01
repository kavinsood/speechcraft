i used the labeling webui for gpt sovits and it was pretty shitty. I am thinkong of creating my own in depth webui for this and put it on github as a piblc project to use. Here is what i want.
1 audio per page
see the waveform of the audio
scroll left right on it
zoom in on the waveform
be able to select, start from anywhere
be able to delete cut and paste selections
view the transcipt below
be able to mark the audio into user created categories
be able to add small empty space at selections
listen in slow motion
transcipt matched to audio. can select a word and it shows where it is in the waveform
be able to split an audio in different parts and have it now as 2 audios
be able to merge 2 different audios together to form 1
be able to edit the transcript
have button to commit the changes (maybe have a history of commits)
has keybinds for fluid usage. one for switching to next audio fast
has a counter showing total duration of dataset which changes when user messes with audio.
shows audio properties like khz etc etc in the corner
it takes in a config file like how gpt sovits take slicer_opt.list

how does this all sound? no code yet. i havnt decided the tech stack or anything

Thought for 24s

This sounds very doable, and also exactly the kind of tool that starts as “just a nicer labeler” and accidentally becomes a full-on DAW-for-datasets (in a good way).

A few thoughts that’ll save you pain later.

What’s great about the feature list

You’re basically covering the three things GPT-SoVITS labeling needs to stop being miserable:

-

Fast review (next/prev keybinds, 1 audio per page, waveform seek, zoom/scroll)

-

Actual editing (split/merge, cut/paste, insert silence, commit/undo history)

-

Text ↔ audio alignment (word click highlights, transcript editing)

That combo is rare in existing dataset tools, which is why people keep suffering with janky UIs.

The “hard parts” (so you design around them)

1) Non-destructive editing vs destructive editing

If you directly rewrite WAV files on every cut/paste/split/merge, you’ll suffer:

-

slow operations

-

quality loss if you re-encode

-

painful undo/redo

Suggestion: store edits as an EDL (Edit Decision List) per clip:

-

“keep [1.24s–3.80s] + insert 80ms silence + keep [4.10s–6.00s]”

-

transcript edits stored as separate deltas

-

only when user hits Commit / Render, you generate new WAV(s)

This also makes “commit history” trivial (each commit is just another EDL snapshot).

2) Word-level “click a word → jump waveform”

This is the single most “wow” feature, and also the most ML-ish:

-

You need word timestamps (forced alignment).

-

Whisper gives segment timestamps; word timestamps depend on the pipeline.

Practical approach:

-

run an aligner locally (as a backend service) that outputs word-level times

-

cache results so UI stays instant

-

UI just consumes: [{word:"hello", start:1.23, end:1.51}, ...]

3) Slow-motion playback (without chipmunking)

Browser playbackRate is easy, but pitch changes.
If you want real slow motion: you’ll need time-stretching (phase vocoder / WSOLA), either:

-

in a backend (best quality, simplest UI)

-

or in-browser via WebAudio + a library / WASM (harder, but neat)

Tech stack options (pick based on where you want it to run)

Option A: “Local web app + backend” (my default rec)

-

Frontend: React + TypeScript

-

Waveform: WaveSurfer-style component (with regions/zoom/scroll)

-

Backend: Python (FastAPI) or Node

-

Audio ops: ffmpeg + rubberband/sox etc server-side

-

Storage: SQLite (clips, transcripts, tags, commits)

Why it’s nice:

-

full file system access

-

easy to run aligners

-

heavy audio processing won’t freeze the UI

Option B: Desktop app (best UX)

-

Tauri (Rust backend, web frontend) or Electron

-

Same frontend ideas, but you get native file access + SQLite cleanly

Option C: Pure browser (hard mode)

Possible, but file write/editing gets awkward unless you rely on:

-

File System Access API (not universal)

-

IndexedDB caching

-

ffmpeg.wasm (big + slower)

If your users are “dataset grinders on Linux,” they’ll love A or B.

A sensible MVP that still feels awesome

If you try to ship everything at once, you’ll drown in edge cases. I’d stage it:

MVP v0.1 (already massively better than the GPT-SoVITS labeler)

-

1 audio per page

-

waveform with zoom/scroll/seek + selection region

-

transcript view + editing

-

tags/categories per clip

-

keybinds: play/pause, jump ±0.5s, next/prev clip, set in/out

-

audio properties display (sr, channels, duration, RMS maybe)

-

dataset duration counter (sum of effective durations if you have EDL)

v0.2 Editing that matters

-

cut/delete selection

-

insert silence at cursor/selection

-

split into two clips (creates 2 EDL outputs)

-

merge clips (concatenate, optional small crossfade)

v0.3 Alignment superpowers

-

run alignment job → word timestamps

-

click word → highlight region

-

drag region boundaries → optionally adjust word timing (advanced)

v0.4 “Commits” that don’t annoy you

-

commit button creates a snapshot:

-

EDL + transcript + tags

-

history sidebar: “commit message”, timestamp, diff summary

-

revert commit / branch (branching is optional; don’t build GitHub inside your app unless you hate weekends)

Data model (keep it boring and you’ll win)

Think in these objects:

-

Project

-

config (imported from slicer_opt.list-like file)

-

label taxonomy

-

Clip

-

original_audio_path

-

current_edl (list of segments + inserts)

-

transcript_current

-

transcript_original

-

tags[]

-

alignment_cache (word timestamps)

-

Commit

-

clip_id

-

parent_commit_id

-

edl_snapshot

-

transcript_snapshot

-

tags_snapshot

-

message, timestamp

This structure makes everything else fall out naturally.

Config file compatibility

Totally reasonable:

-

Support the existing GPT-SoVITS style config as an input

-

Internally convert it to JSON

-

Export outputs in formats people already use (e.g., jsonl manifest + wavs)

Bonus points if you also ship a tiny CLI:

-

render_project --commit latest --out ./processed_wavs
So power users can batch-render without clicking around.

My overall verdict

Feature list: 🔥
Main risk: trying to implement “DAW-grade editing” destructively and/or without an edit model (EDL). If you go EDL + commit snapshots, your life gets 10x easier.

If you want, I can propose:

-

a clean keybind map (so it feels like an editor, not a mouse-only toy)

-

a repo structure + modules (frontend/backend/shared types)

-

what to implement first so you get a working prototype fast without painting yourself into a corner

we'll go EDL then. also judge this idea practically compared to other tools out there. My personall opinion is this is great compared to existing options

Thought for 47s

EDL is the right call — it turns this from “web audio editor (pain)” into “dataset editor with deterministic rendering (nice).”

Practical reality check vs what already exists

You’re not imagining it: most existing tools cover pieces of your workflow, but almost none cover the whole loop (label → edit → align → split/merge → export manifests → iterate fast).

1) Classic waveform + labels tools (great at some things)

-

Audacity: excellent waveform editing + labeling (label tracks, regions), but it’s not transcript-centric, and it won’t manage dataset metadata / categories / per-clip history the way you want.

-

Praat: strong phonetics-style annotation with TextGrid tiers (waveform + time intervals + text labels), but it’s not designed as a modern “dataset assembly line,” and UX/keybind flow is… academic.

-

ELAN: very good multi-tier annotation for audio/video, but it’s an annotation tool, not a practical audio-editing/splitting/merging workstation.

Takeaway: these are great if you’re willing to glue things together with scripts and manual bookkeeping. Your idea is basically “stop gluing.”

2) Data labeling platforms (good at annotation management, weak at editing)

-

Label Studio can show waveform and do audio transcription/segmentation labeling, but the “advanced audio transcription interface” emphasis is largely positioned as an Enterprise feature, and it’s not trying to be a DAW with cut/paste/insert-silence/split/merge.

Takeaway: good for teams and labeling ops; not built for the gritty TTS dataset “surgery” you described.

3) “Text-first” editors (closest vibe, but wrong target)

-

Descript is the big reference point for “edit audio by editing words,” but it’s commercial/proprietary and not dataset-pipeline focused (export manifests, keep originals immutable, reproducible renders, etc.).

Takeaway: it proves the UX is valuable — but it’s not the right tool for open-source dataset prep and reproducibility.

4) Web-based speech annotation that’s actually serious

-

EMU-webApp is a legit web app for labeling/visualizing/correcting speech data (and it’s open-source).

Takeaway: this is probably the closest category competitor. Your differentiator would be: EDL editing + dataset ops (split/merge/render) + commit history + TTS-friendly exports.

5) Alignment tooling (you’ll likely integrate, not replace)

-

Montreal Forced Aligner (MFA) outputs word/phone tiers as TextGrids (excellent for word→time mapping).

-

WhisperX provides word-level timestamps via forced alignment-ish refinement, but you can see real-world issues/discussion around timestamp accuracy depending on settings/data.

Takeaway: don’t bet the whole UX on one aligner being perfect; design it so alignment is pluggable and correctable.

So… is your idea “practically great”?

Yes — if you frame it as a dataset workstation rather than “a better labeler.”

Your unique value proposition (what’s missing in most tools):

-

Non-destructive EDL editing + deterministic renders (reproducible dataset building)

-

Dataset-native operations: split/merge clips, insert silence, duration counter updates, config-driven workflows

-

Transcript ↔ waveform tight coupling with word-click navigation (powered by an aligner)

-

Keybind-first throughput (annotation tools rarely feel like Vim for audio)

-

History/commits at the clip/project level (annotation tools track annotations, not edits as versioned transformations)

That combo is rare enough that people will genuinely use it, especially TTS folks who currently bounce between Audacity/Praat + scripts + spreadsheets.

The “don’t accidentally rebuild Ableton” warning

If you keep the scope anchored to:

-

speech clips (not multitrack music)

-

EDL + render on commit

-

alignment as a plugin

-

exports that match common pipelines (TextGrid / JSONL manifests / wav + txt)

…then it stays very practical and shippable, and it won’t turn into a forever-project.

If you want, I can also tell you what minimum feature set would beat the existing ecosystem on day 1, and what would be “nice but will eat your soul” to implement.

Sources

wwhats EDL, render and alignment again?

Sure — in this context they’re just the 3 “pillars” of how your tool would work.

EDL (Edit Decision List)

Think of EDL as a recipe of edits, not the edited audio itself.

Instead of permanently chopping the WAV, you store instructions like:

-

keep audio from 0.80s → 2.40s

-

delete 2.40s → 2.65s

-

insert 120 ms silence at 3.10s

-

then keep 3.10s → 6.00s

So the original file stays untouched, and your app just remembers “what the user wants the final clip to be.”

Why it’s useful:

-

instant undo/redo

-

no repeated re-encoding

-

easy split/merge (EDL can describe both)

-

easy “commit history” (store EDL versions)

Render

Rendering is when you apply the EDL recipe to actually produce output audio files.

Example:

-

you edited clip_014.wav using EDL

-

on Commit / Render, the app generates:

-

clip_014_edited.wav

-

updated transcript / manifest entries

So: EDL = instructions, render = cooking the meal.

Alignment

Alignment is how you connect words in the transcript to timestamps in the audio.

Output looks like:

-

“hello” → 1.23s to 1.51s

-

“world” → 1.52s to 1.88s

Then your UI can do the cool stuff:

-

click a word → jump/highlight that region on waveform

-

select region on waveform → highlight the corresponding words

How you usually get alignment:

-

run a forced aligner (common in speech work)

-

or use an ASR model that gives word timestamps, optionally refined with alignment

If you remember only one line:
EDL = what you changed, Render = make the new audio, Alignment = map words ↔ time.

whats your recommended tech stack considering i wanna have waveforms and then zoom in on em as well

Thought for 32s

If your #1 priority is waveform UX (scroll/zoom/regions/word-highlights) and you want this to feel like a “real app” for dataset grinding, here’s what I’d pick.

Recommended stack (practical + fast to build)

App shape: Desktop-first

Tauri v2 + React + TypeScript

Why:

-

You get a clean “web UI” dev experience, but with native file access and a real local app feel (no browser sandbox weirdness). Tauri’s v2 file system plugin is the straightforward way to do local reads/writes.

Waveform engine: wavesurfer.js

wavesurfer.js + @wavesurfer/react

Why:

-

Built for interactive waveform rendering & playback.

-

Has official plugins you’ll want immediately:

-

Regions (your selections)

-

Timeline

-

Minimap (acts like a scrollbar / overview)

-

Spectrogram (optional but super useful for spotting noise)

-

It has a real zoom system, and the docs call out that super-fluid zoom can be CPU-heavy (which is exactly the kind of “gotcha” you want surfaced early).

-

Actively released recently (that usually correlates with fewer “stuck on old browser APIs” surprises).

-

React wrapper exists from the same author, so integration isn’t cursed.

What you’d implement on top: your EDL + dataset logic, and your own region semantics (cut/paste/insert silence/split/merge) using regions as the UI handles.

Backend for “Render / Commit”

You have two clean paths:

Path A (simple + common):

-

Tauri (Rust commands) calling ffmpeg for final rendering, concatenation, silence insertion, resampling, etc.

Path B (ML-friendly):

-

Tauri + a Python sidecar (FastAPI or just CLI calls) that handles:

-

forced alignment jobs

-

any audio analysis you want (SNR, silence detection, etc.)

-

rendering if you prefer Python tooling

(You can start with A and add B later when alignment enters.)

Storage

-

SQLite project DB:

-

clip list

-

EDL per clip

-

tags/categories

-

transcript versions

-

commit history

This is the difference between “cool demo” and “I can process 5,000 clips without losing my mind”.

Strong alternative (especially if you want overview+zoom built-in)

Peaks.js

Peaks.js was designed around the “overview waveform + zoomed waveform” interaction and supports zoom/scroll and segment markers.
It’s still alive (beta releases in 2025).

When I’d choose Peaks.js over wavesurfer:

-

You really want the dual-view UX as a first-class concept from day 1

-

You want “segments/markers” as the core primitive

When I’d stick with wavesurfer:

-

You want the plugin ecosystem + React wrapper + broader “app-like” feature set out of the box.

My pick for you

Tauri v2 + React/TS + wavesurfer.js + SQLite + ffmpeg
…and later add a Python sidecar for alignment.

That stack hits your needs:

-

waveform zoom/scroll/seek/regions ✅

-

keybind-heavy UI ✅

-

non-destructive EDL + render on commit ✅

-

alignment pipeline without rewriting the app ✅

If you want, I can outline a minimal component architecture (WaveformPane, TranscriptPane, EDL model, Command palette/keymap) so you don’t end up with a 4,000-line EditorPage.tsx that future-you wants to delete from orbit.

Sources

first of all, break this down into the usecase and different 'blocks'

I’m going to turn your idea into a product map: first the user workflow, then the major system blocks, then how they connect.
Thought for 5s

Yep — this is the right next step.

Don’t think of it as “a big app with a lot of features.”
Think of it as:

one core use case
split into
a few clear product blocks

1. Core use case

The app’s real use case is:

prepare and correct speech dataset clips for TTS / voice model training quickly and safely

That means a user wants to:

-

open one clip

-

inspect audio visually

-

compare it against transcript

-

fix mistakes in audio and/or text

-

categorize it

-

move to next clip fast

-

keep everything reproducible

So the app is not mainly:

-

a music editor

-

a general audio workstation

-

a generic annotation platform

It is mainly:

a dataset editing workstation for speech clips

2. Main user jobs

These are the actual jobs the user is trying to do.

A. Review a clip

User wants to ask:

-

is this clip clean?

-

is the transcript correct?

-

are boundaries good?

-

is there weird silence / breath / noise / cutoff?

B. Edit a clip

User wants to:

-

cut bad parts

-

insert silence

-

split clip

-

merge clips

-

re-order / adjust sections

-

maybe slow playback to inspect details

C. Align text and audio

User wants to:

-

see where words happen

-

click transcript and jump to waveform

-

verify word timing

-

correct transcript while seeing timing context

D. Organize dataset

User wants to:

-

label clips by category

-

track dataset duration

-

see sample rate / channels / duration

-

quickly move through many files

E. Save work safely

User wants to:

-

commit changes

-

undo mistakes

-

keep history

-

render final outputs later

3. Product blocks

Here’s the clean breakdown.

Block 1: Project / Dataset Loader

Purpose:
load a dataset/project and understand its structure

Responsibilities:

-

open project folder

-

read config file (slicer_opt.list-style input or your own format)

-

load clip list

-

load transcript data

-

load categories/tags config

-

calculate total dataset duration

This block answers:

-

what files exist?

-

what transcript belongs to what clip?

-

what settings define this project?

Block 2: Clip Navigator

Purpose:
move through clips fast

Responsibilities:

-

next / previous clip

-

search/filter clips

-

mark clip status (untouched / edited / needs review / done)

-

show clip index and progress

-

keybind for next clip fast

This is the “throughput” block.
Without this, the app feels slow even if the editor is good.

Block 3: Waveform Viewer

Purpose:
show audio visually and make inspection easy

Responsibilities:

-

render waveform

-

zoom in/out

-

scroll left/right

-

playhead/cursor

-

click to seek

-

waveform selection

-

optional minimap/overview

-

audio properties display

This is the visual foundation of the app.

Important distinction:
this block is mostly view + interaction
not the true editing engine.

Block 4: Playback / Transport

Purpose:
control listening

Responsibilities:

-

play/pause

-

seek

-

loop selection

-

slow playback

-

jump forward/backward

-

maybe frame/small-step nudging

Why separate from waveform viewer?
Because transport logic is its own thing:

-

keyboard control

-

speed control

-

loop behavior

-

current position state

Block 5: Selection & Region System

Purpose:
turn waveform interaction into meaningful editable ranges

Responsibilities:

-

define selected range

-

create/edit regions

-

set in/out points

-

snap or not snap

-

show active selection boundaries

-

allow selection starting from anywhere

This is the layer between “I clicked on the waveform”
and
“I want to delete this section.”

Block 6: EDL Engine

Purpose:
store edits non-destructively

Responsibilities:

-

represent keep/cut/insert-silence/split/merge actions

-

calculate resulting clip duration

-

support undo/redo

-

support history snapshots

-

apply edits logically without rewriting original immediately

This is the heart of the app.

Without this block, everything becomes destructive and messy.

Think:

-

original audio stays untouched

-

this block stores the recipe of changes

Block 7: Transcript Editor

Purpose:
view and edit text tied to the clip

Responsibilities:

-

show transcript below waveform

-

inline text editing

-

save revised transcript

-

maybe version transcript separately from audio edits

-

highlight transcript portions

This should feel lightweight and fast, not like editing in a spreadsheet.

Block 8: Alignment Layer

Purpose:
connect words to times in audio

Responsibilities:

-

store word-level timestamps

-

click word → jump waveform

-

click region → highlight words

-

update alignment display after edits

-

show confidence or missing alignment if needed

Important:
this block does not necessarily generate alignment itself.
It may just consume alignment data from an aligner.

So internally this block may have 2 subparts:

-

alignment data model

-

alignment visualization

Block 9: Audio Edit Operations

Purpose:
expose actual user actions

Responsibilities:

-

delete selection

-

cut selection

-

paste selection

-

insert silence

-

split clip into two

-

merge two clips into one

This block uses:

-

selection system

-

EDL engine

-

clip/project state

This is where user commands live.

Block 10: Clip Metadata / Tagging

Purpose:
let user classify and annotate clips

Responsibilities:

-

assign categories/tags

-

user-created labels

-

maybe notes/issues flags

-

store review state

Examples:

-

clean

-

noisy

-

clipped ending

-

emotional

-

whisper

-

bad transcript

-

needs re-record

This becomes super valuable later for filtering/export.

Block 11: Commit / History System

Purpose:
save meaningful states of work

Responsibilities:

-

commit changes button

-

snapshot EDL + transcript + metadata

-

commit messages maybe

-

history browsing

-

undo/redo vs commit history distinction

-

revert to earlier commit

Important distinction:

-

undo/redo = immediate editing actions

-

commit history = saved milestones

Block 12: Render / Export System

Purpose:
turn non-destructive edits into real outputs

Responsibilities:

-

apply EDL to generate final audio

-

export split clips

-

export merged clips

-

export transcripts/manifests

-

maybe export alignment metadata

-

maybe resample / normalize if needed

This is where edited project becomes usable by GPT-SoVITS or other pipelines.

Block 13: Stats / Dataset Summary

Purpose:
give project-level awareness

Responsibilities:

-

total dataset duration

-

effective duration after edits

-

clip count

-

edited/unedited counts

-

maybe duration by category

-

sample rate / bit depth summary

This gives the “dataset management” feel instead of just “clip editing.”

Block 14: Input/Keybind System

Purpose:
make the app fast for real users

Responsibilities:

-

keyboard shortcuts

-

mode-aware shortcuts

-

prevent conflicts with text editing

-

next/prev clip

-

play/pause

-

set selection points

-

delete selection

-

zoom controls

This is easy to underestimate, but it’s one of the main differentiators.

4. Best high-level grouping

If you want the cleanest architecture, group those blocks into 4 big zones.

Zone A: Project layer

-

Project / Dataset Loader

-

Clip Navigator

-

Metadata / Tagging

-

Stats / Dataset Summary

This handles dataset-wide state.

Zone B: Editor layer

-

Waveform Viewer

-

Playback / Transport

-

Selection & Region System

-

Transcript Editor

-

Alignment Layer

This is the user-facing editing workspace.

Zone C: Edit logic layer

-

EDL Engine

-

Audio Edit Operations

-

Commit / History System

This is the logic core.

Zone D: Output layer

-

Render / Export System

This turns edits into real files.

5. Practical screen layout blocks

On the actual UI, I’d map it like this:

Top bar

-

project name

-

current clip name

-

clip index / total

-

commit button

-

undo/redo

-

next/prev

Left sidebar

-

clip list / filters / tags / status

Main center

-

waveform

-

minimap/overview

-

transport controls

-

zoom controls

Lower center

-

transcript editor

-

word timing highlights

Right sidebar

-

clip metadata

-

audio properties

-

categories

-

notes

-

alignment info maybe

Bottom/status bar

-

dataset total duration

-

current selection duration

-

playback speed

-

sample rate / channels

-

current mode / keybind hints

6. Most important dependency chain

This is the real build order logic:

-

Project loader

-

Clip navigation

-

Waveform viewer

-

Playback

-

Selection system

-

EDL engine

-

Basic transcript editor

-

Edit operations

-

Commit/history

-

Alignment

-

Render/export

-

Stats + polish

That order matters because:

-

waveform without selection is only half useful

-

selection without EDL becomes messy

-

edits without history are dangerous

-

alignment is powerful, but not the first foundation

7. The minimum viable “blocks” for v1

If you want the leanest real first version, the essential blocks are:

-

Project loader

-

Clip navigator

-

Waveform viewer

-

Playback

-

Selection system

-

EDL engine

-

Transcript editor

-

Basic edit ops

-

Metadata/tags

-

Commit/history

-

Export/render

Then add alignment after.

That would already be stronger than a lot of current tooling.

8. The one-line summary

Your app breaks down into:

dataset management + clip editor + transcript/alignment workspace + non-destructive edit engine + export pipeline

That’s the clean mental model.

Next best step is to turn this into either:

-

feature priority tiers
or

-

frontend components vs backend services

I’d do that next, because that’s where the architecture starts becoming obvious.

there are two threads i want to go down

- the stack required to implement this
- specifically the audio manipulation libraries used

let's discuss both. we are going to need to do research and uncover my hidden expectations (You have to think as a founding product manager to understand my needs and best research the libraries for this)

about the stack

i thought about this a bunch, and ... i'm pivoting from a desktop app

i honestly don't need the desktop app, the plus point is, that users can ssh and get the webapp working that way too, but considering desktop app, they cant. and i wouldn't mind if the thing was running in my browser. it saves me a ton of pain.
Eliminates dependency hell: when i introduce the desktop app, either i use the sidecar approach which i have to manage, but honestly, as I'm dealing with AI libraries that are massive and hardware dependent, WebUI allows us to control the environment on a server/workstation rather than fighting with a user’s local Python installation or CUDA drivers.
Decoupled Hardware Requirements: By moving to the web, the heavy lifting (AI inference) can run on a high-GPU server, while the user accesses the interface via a standard browser on any device.
Effortless Remote Access: A WebUI supports SSH Tunneling out of the box. Users can easily run the backend on a remote compute instance and interact with the app locally, which is nearly impossible with a standard desktop binary.
Reduced Bundle Size: Instead of shipping a 500MB+ desktop installer that includes a portable Python runtime, we can provide a lightweight Docker image or a simple pip install for the server side.
Faster Development Cycle: Using tools like Gradio or FastAPI, we can build and iterate on the UI entirely in Python. We avoid the overhead of learning Rust (Tauri) or Zig (Electrobun) and managing the IPC bridge between different languages.
Cross-Platform by Default: We no longer need to compile, sign, and notarize separate binaries for Windows, macOS, and Linux. If it has a browser, it can run the app.

so, now, talking about the webapp-based structure, ideally the backend can be in python because the scripts we've built so far (Diarization, and other batch tasks on the datasets), a fastAPI server probably

the UI could be a regular vite/react sort of thing, that's working with the fastapi backend

honestly that works, i'd want a simpler way for users to manage this, a cli for upping it, whatever

what's interesting and i'd like to explore is

if the audio based libraries, some are better than in python, if something much better is there in go, then let's use that.

essentially, right now, i'd want for you to write clearly what things do we need through libraries, and then a language-agnostic deep research on the best ones for it2/2

I’m mapping this into two parallel tracks: the product/stack decision, and the audio pipeline decision. I’m also looking for the hidden constraint that matters most for a browser app here: remote waveform performance over SSH, not just “can it render audio at all.”

I’ve got the core picture now: this is not “an ML demo UI,” it’s a precision editor that happens to trigger ML jobs. That pushes the frontend away from Gradio and toward a real React editor, while the backend stays Python because your AI/data tooling already lives there.

I’ve finished the library sweep, including the part you implicitly care about most: not just “what can edit audio,” but “what survives a browser-over-SSH workflow without feeling laggy.” The answer is heavily shaped by precomputed peaks, HTTP range serving, and keeping FFmpeg as the render truth.
Thought for 6m 18s

The browser pivot is the right one.

For this product, the hard part is not shipping a desktop shell. It is making a remote, GPU-backed speech editor feel fast and trustworthy. Gradio is built for quickly turning Python functions/models into web apps, while FastAPI is a production API framework with WebSocket and deployment support; that makes a custom React frontend + FastAPI backend a much better fit for a precision editor than a Python-only demo UI.

What you’re really building is this:

-

a browser editor for waveform + transcript interaction

-

a server-side render/alignment engine

-

a project system that keeps edits reproducible

That uncovers a few hidden expectations you probably already have:

-

It must feel good over SSH tunnel / remote browser access

-

It must not require shipping CUDA / Python / model dependencies to the client

-

Waveforms should appear fast without decoding huge files in the browser

-

Playback seeking should work without re-downloading the whole file

-

Alignment should be correctable, not treated as magic truth

-

Edits must be non-destructive and renderable later

That last point is why your EDL choice is so important.

The stack I’d recommend

Frontend

React + TypeScript + Vite

Why:

-

You need custom editor interactions, not just forms and outputs.

-

You’ll want precise control over waveform state, selections, keybinds, transcript highlighting, and commit/history behavior.

-

The ecosystem around custom canvas/Web Audio/waveform tooling is best here.

For app state, I’d keep it simple:

-

Zustand for editor-local state

-

TanStack Query for server state / async jobs / cache

Both are designed around the exact kind of “local editor state + async backend state” split you’ll have.

Backend

FastAPI + Pydantic, with a separate worker process for heavy jobs.

Why:

-

FastAPI is fast, typed, and production-oriented.

-

WebSockets are officially supported, which is useful for progress updates during alignment/render jobs.

-

Typer is a natural companion for the CLI you want.

Storage

SQLite for project metadata and history, with files on disk for audio/peaks/renders.

Use SQLite for:

-

clips

-

EDLs

-

transcripts

-

word timings

-

tags

-

commit history

SQLModel is reasonable if you want a Pythonic model layer on top of SQLite without writing too much boilerplate.

Serving audio to the browser

A subtle but important point:

You do not want to upload full files into the browser app and manipulate them there.
You want the browser to request audio like a normal media player, with seeking.

That works well on the web because:

-

HTTP range requests let clients request parts of a resource

-

Starlette FileResponse supports range requests directly, returning 206 Partial Content when the client seeks into a file

That is a big reason the webapp route is actually strong for remote use.

What capabilities you need libraries for

Here is the capability map, independent of language.

1. Waveform visualization and interaction

You need:

-

draw waveform

-

zoom in/out

-

scroll/overview

-

playhead and selection regions

-

click-to-seek

-

word highlight overlays later

Best candidates:

wavesurfer.js

It is an interactive waveform rendering/playback library with plugins for Regions, Timeline, Minimap, and Spectrogram. Its docs also explicitly recommend pre-decoded peaks for large files, because decoding whole audio in-browser can fail due to memory constraints; streaming support also relies on pre-decoded peaks and duration.

Peaks.js

Peaks.js is more opinionated around the exact UX you want: overview waveform + zoomed waveform. Its docs say waveform data can be precomputed with audiowaveform, and that precomputing is preferable for longer files because browser-side waveform generation downloads the whole file and is CPU-intensive. It also prefers binary waveform data because it is smaller even after compression.

My verdict

-

Pick wavesurfer.js if you want the smoother path, bigger ecosystem, more common usage, and plugin flexibility.

-

Pick Peaks.js if your product identity is strongly “overview + zoom pane editor” from day one.

For your feature list, I lean wavesurfer.js first, unless the dual-pane overview/zoom interaction is central to your UX identity.

2. Peak-data generation for fast waveform loading

This is one of the most important hidden requirements for a browser-over-SSH app.

audiowaveform

audiowaveform generates waveform data from audio files and outputs either .dat or .json. Ongoing development moved to Codeberg, and the GitHub mirror shows a latest release in August 2025. Peaks.js directly recommends it, and wavesurfer’s docs also point to it for pre-generated peaks.

My verdict

Use audiowaveform regardless of whether you choose wavesurfer or Peaks.js.

This is the main thing that will make remote waveform UX feel snappy.

3. Core audio render/manipulation engine

This is the engine that turns EDL into actual audio.

FFmpeg

This should be your render truth.

FFmpeg already covers most of the edit primitives you need:

-

atrim for selecting kept ranges

-

adelay and silence-filled delay behavior for insert-style operations

-

apad for padding with silence

-

anullsrc for generating silence sources

-

concat for joining segments/clips

-

acrossfade for nicer merges

-

loudnorm for loudness normalization

-

silencedetect for silence analysis

My verdict

Use FFmpeg as the source of truth for render/export, even if you use Python or another language around it.

This is the strongest place in the whole stack to stay conservative.

4. Slow motion / time-stretching

You said “listen in slow motion,” and there are really two meanings:

In-browser preview

The browser already supports changing playback rate. HTMLMediaElement.playbackRate is built-in and widely available.

That is enough for a first-pass preview control.

High-quality offline time-stretch / pitch-aware processing

For better quality, use:

Rubber Band Library

Rubber Band is specifically for high-quality time-stretching and pitch-shifting. It supports offline/sample-exact processing and is also available in FFmpeg via the rubberband filter when FFmpeg is built with --enable-librubberband.

My verdict

-

Browser preview: use native playbackRate first

-

Final render / “serious slow motion”: use Rubber Band

5. Metadata and file inspection

You want to show:

-

sample rate

-

channels

-

codec

-

duration

-

maybe bit depth, bitrate, tags

ffprobe

FFprobe is the clean default for stream metadata and technical info.

MediaInfo

MediaInfo is useful if you want richer technical/tag display in the UI, beyond just what you need for editing.

My verdict

Use ffprobe for app logic, optionally MediaInfo for “details” panes if you want richer presentation.

6. Python-side audio I/O and analysis

This is where people often overcomplicate things.

python-soundfile

Good for simple read/write and block-wise access in NumPy.

librosa

Good for analysis and helper tasks like:

-

silence / non-silent interval detection

-

trimming

-

feature extraction

-

quick signal analysis

Its docs explicitly expose trim, split, and analysis tools.

PyAV

Good when you need lower-level frame/container access from Python and want direct FFmpeg bindings rather than shelling out for every operation.

My verdict

-

Use FFmpeg for render

-

Use ffprobe for metadata

-

Use librosa/soundfile for analysis and helper tasks

-

Use PyAV only if you discover you need packet/frame-level control

Do not make librosa or soundfile your render engine.

7. ML-oriented decoding / tensors

If your backend jobs want tensors directly for models:

TorchCodec

TorchCodec decodes audio/video into PyTorch tensors on CPU/CUDA and now also has audio/video encoding docs. Torchaudio itself has transitioned into a maintenance phase and its older I/O/forced-alignment APIs have been deprecated or removed, with docs recommending native TorchCodec usage instead.

My verdict

If you need tensor-native decoding for ML jobs, use TorchCodec, not old torchaudio I/O as your long-term base.

8. Alignment libraries

This deserves a very opinionated answer.

Montreal Forced Aligner (MFA)

MFA is the strongest “serious corpus alignment” option here. Its docs define forced alignment as taking text + audio and producing a time-aligned version using a pronunciation dictionary; its output includes TextGrids with word and phone tiers. It supports pretrained models and multiple languages.

WhisperX

WhisperX explicitly offers fast ASR with word-level timestamps and diarization. That makes it very appealing as a bootstrap aligner. But there are active reports in 2025 comparing its timestamps unfavorably to MFA in some cases, so I would not make it your only “ground truth” alignment backend.

Torchaudio forced alignment

Torchaudio had forced-alignment APIs, but the docs now mark them deprecated as part of the maintenance-phase transition.

My verdict

-

Primary alignment backend: MFA

-

Bootstrap / quick-start alignment: WhisperX

-

Do not base the core alignment system on torchaudio forced alignment

That gives you the right product behavior:

-

quick first pass from WhisperX if needed

-

stable “real alignment” from MFA

-

manual correction in your UI

9. Is Go worth it anywhere?

Bluntly: not for the core media stack.

Go has decent audio libs, but they are not the strongest match for a speech-editing/rendering/alignment product:

-

faiface/beep is nice for playback/basic processing

-

go-audio/wav was archived in February 2026

-

serious media work in Go often ends up as wrappers around FFmpeg anyway (gmf, etc.)

Rust is more interesting if you ever want a custom decode core; Symphonia is a solid pure-Rust decoder/demuxer. But even there, FFmpeg still wins on full editing/filter-graph breadth.

My verdict

Stay with:

-

JS/TS in frontend

-

Python in backend/orchestration

-

FFmpeg / Rubber Band / audiowaveform / MFA as the heavy native tools

That is the least risky, highest-power combination.

The stack I would actually ship

Frontend

-

React + TypeScript + Vite

-

wavesurfer.js for waveform UI

-

audiowaveform for precomputed peaks

-

Zustand for editor state

-

TanStack Query for backend async state

Backend

-

FastAPI

-

WebSockets for progress

-

Starlette/FileResponse for audio serving with range requests

-

Typer CLI for serve, scan, align, render, doctor

-

SQLite project DB, optionally via SQLModel

Audio/ML toolchain

-

ffprobe for metadata

-

FFmpeg for render/export

-

Rubber Band for high-quality slow-motion / pitch-aware stretching

-

librosa + soundfile for analysis utilities

-

MFA for primary alignment

-

WhisperX as optional bootstrap

-

TorchCodec only where tensor-native ML decoding is useful

One product-manager-style recommendation you probably won’t regret

Do not make the frontend depend on live waveform generation from raw audio in the browser.
Make peak generation a backend/cache step.

That single decision lines up with all your real constraints:

-

remote access

-

browser UX

-

large environments on server

-

fast zoom/scroll

-

no repeated full decode in client

And it keeps your product feeling like a tool, not a tech demo.

Final recommendation

If I had to lock it today, I would choose:

React/Vite frontend + FastAPI backend + wavesurfer.js + audiowaveform + FFmpeg + Rubber Band + MFA + SQLite + Typer

And I would only switch one thing depending on UX preference:

-

choose Peaks.js instead of wavesurfer.js if the product’s signature interaction is overview pane + zoom pane

-

otherwise stay with wavesurfer.js

The next most useful step is to turn this into a capability-by-capability shortlist with:

-

must-have library

-

backup option

-

why it wins

-

where it plugs into the architecture

Sources
