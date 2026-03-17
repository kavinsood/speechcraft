Automated High-Purity Speaker Extraction and Few-Shot Voice Cloning for Natural Roleplay Speech Synthesis


---

I. Introduction to Problem (What, Why, How)

1. What is the Problem?

Recent advances in speech AI have made it possible to clone a person’s voice using only a small amount of speech data. Modern voice-cloning systems such as GPT-SoVITS and CosyVoice 3 can generate natural speech with high speaker similarity, making them useful for applications such as virtual assistants, narration, dubbing, character roleplay, and conversational agents.

However, building a high-quality voice clone still faces major practical problems. The most important issue is not only model training, but also dataset purity. Real-world recordings such as podcasts, interviews, and conversations contain:

multiple speakers,

overlapping speech,

noise and interruptions,

inconsistent speaking styles,

background artifacts.


Voice-cloning models require clean single-speaker data. If the dataset contains speech from other speakers or overlapping voices, the cloned output becomes unstable, unnatural, or inaccurate. Therefore, the main challenge addressed in this project is to create an automated system that extracts only the target speaker’s voice from multi-speaker recordings and then use that data for few-shot voice cloning.

This project also studies whether a zero-shot cloning system like CosyVoice 3 can provide results close to or better than a fine-tuned system like GPT-SoVITS, especially for roleplay and expressive TTS use cases.

2. Why is this Problem Important?

Voice cloning is becoming important in many domains, such as:

conversational AI,

digital characters and roleplay systems,

personalized voice assistants,

media production and dubbing,

accessibility and narration tools.


For these applications, the generated voice must be:

natural and human-like,

highly similar to the target speaker,

free from artifacts,

expressive across different tones and emotions,

reliable in text pronunciation and content delivery.


Manual preparation of training data is slow and expensive. Identifying the correct speaker, removing interruptions, and verifying purity by hand is not scalable. Therefore, an automated pipeline that can produce high-purity speaker datasets from long conversational recordings is highly valuable.

This problem is also important from a research perspective because it connects three active areas of AI:

speech processing and speaker diarization,

speaker verification and embedding-based filtering,

few-shot and zero-shot voice cloning.


3. How Will the Problem Be Addressed?

The project addresses this problem through a complete machine learning and audio-processing pipeline consisting of:

audio preprocessing and standardization,

multi-speaker diarization,

reference-based speaker identification,

overlap removal,

speaker-verification-based filtering,

export of training-ready audio,

training and evaluation of voice-cloning systems.


The pipeline uses NVIDIA NeMo diarization, Titanet speaker verification, PyTorch, ffmpeg, and GPU acceleration on an NVIDIA H200 141 GB GPU.

The extracted clean speech data is then intended for use in cloning systems such as GPT-SoVITS and CosyVoice 3. The project also examines the tradeoff between:

CosyVoice 3 zero-shot cloning using short reference clips, and

fine-tuned voice cloning using larger speaker-specific datasets.


Nature of the Project

This project is:

Research Based
because it studies speaker diarization, speaker verification, dataset purity, and the comparative effectiveness of zero-shot and fine-tuned voice-cloning systems.

Application Based
because it develops a working end-to-end system that extracts a target speaker and produces a usable dataset for real voice-cloning deployment.

Hardware Based
because it relies on GPU-accelerated speech processing and model training using high-performance compute infrastructure, especially the NVIDIA H200 GPU.


---

II. Literature Survey

This section reviews important recent software, methods, and hardware relevant to the project.

1. NVIDIA NeMo

NVIDIA NeMo is a toolkit for conversational AI, ASR, TTS, speaker diarization, and speaker recognition.

Pros

Integrated support for diarization and speaker models

Strong GPU acceleration

Stable ecosystem for production and research

Supports RTTM-based diarization outputs

Easy integration with PyTorch workflows


Cons

Setup can be resource intensive

Some pretrained pipelines are specialized for telephonic or limited domains

Requires careful configuration for best diarization quality


2. PyAnnote Audio

PyAnnote is a widely used open-source toolkit for speaker diarization and segmentation.

Pros

Strong research reputation in diarization

Good support for segmentation pipelines

Large adoption in academic work

Flexible for experimentation


Cons

Version compatibility issues are common

API changes can break pipelines

Extra dependencies increase fragility

More difficult to maintain in some production settings


3. GPT-SoVITS

GPT-SoVITS is a few-shot voice cloning framework designed to generate speaker-specific speech with relatively small datasets.

Pros

Works with small amounts of speaker data

Popular in the open-source voice-cloning community

Good speaker identity retention after fine-tuning

Effective for custom voice adaptation


Cons

Sensitive to dataset cleanliness

Fine-tuning can be unstable

More engineering effort required than zero-shot systems

Emotional generalization depends heavily on training data diversity


4. CosyVoice 3

CosyVoice 3 is a modern speech generation and voice-cloning system with strong zero-shot capabilities.

Pros

Strong zero-shot cloning performance

Better text faithfulness and intelligibility in reported comparisons

Lower need for large speaker-specific datasets

Good for rapid deployment and lower engineering overhead


Cons

Fine-tuning gains may be only marginal in some cases

Risk of regressions after speaker-specific adaptation

May still need careful prompt/reference design for expressive roleplay use cases


5. Titanet Speaker Verification Model

Titanet is used for speaker verification through embedding similarity.

Pros

Strong speaker discrimination

Useful for filtering diarization errors

Works well with reference-based speaker matching

Lightweight compared to full generative training pipelines


Cons

Very short segments can produce unstable embeddings

Threshold selection affects precision and recall

Performance depends on the quality of reference audio


6. ffmpeg

ffmpeg is used for audio decoding, format conversion, and slicing.

Pros

Fast and reliable

Supports nearly all audio formats

Essential for preprocessing and export

Easy to integrate into automated scripts


Cons

Command syntax can be complex

Quality issues can arise if conversion settings are incorrect

Purely a media tool, not intelligent filtering


7. NVIDIA H200 GPU

The NVIDIA H200 is a high-memory GPU used for large-scale speech processing and model training.

Pros

141 GB VRAM provides large memory headroom

High tensor performance for deep learning

Suitable for diarization, speaker verification, and model fine-tuning

Reduces training and preprocessing time significantly


Cons

Very expensive hardware

Requires proper CUDA environment and server support

Underutilized if data pipelines are not optimized



---

III. Comparative Study

Technology / Method	Main Use	Advantages	Limitations	Relevance to Project

NVIDIA NeMo	Diarization and speaker processing	Integrated, GPU-accelerated, stable	Requires tuning and resources	Core extraction pipeline
PyAnnote Audio	Speaker diarization	Strong research utility, flexible	Fragile compatibility, API issues	Initial approach, later replaced
Titanet	Speaker verification	Good identity matching, embedding-based filtering	Less stable on very short clips	Speaker purity filtering
GPT-SoVITS	Few-shot voice cloning	Strong speaker adaptation with small data	Sensitive to noisy data and training choices	Fine-tuned cloning baseline
CosyVoice 3	Zero-shot / adaptive cloning	Strong zero-shot performance, lower setup effort	Fine-tuning may not justify cost	Main comparison target
ffmpeg	Audio preprocessing	Fast, universal, reliable	Non-intelligent media processing only	Input standardization and slicing
NVIDIA H200 GPU	Hardware acceleration	Large VRAM, fast training/inference	Expensive infrastructure	Enables fast full pipeline execution



---

IV. Objective

The major objectives of the project are:

1. To develop an automated pipeline for extracting a single target speaker from multi-speaker conversational audio with high purity.


2. To remove overlapping speech and minimize contamination from other speakers in order to prepare training-ready datasets for voice cloning.


3. To use speaker diarization and speaker verification methods to identify, filter, and validate target-speaker audio automatically.


4. To generate clean and auditable datasets suitable for few-shot voice cloning models such as GPT-SoVITS and CosyVoice.


5. To compare the practical usefulness of CosyVoice 3 zero-shot cloning and fine-tuned voice-cloning systems for natural roleplay-oriented TTS.


6. To analyze the tradeoff between additional training effort and actual improvement in speaker quality, naturalness, and robustness.


7. To utilize GPU hardware efficiently for preprocessing, filtering, and model training tasks.




---

V. Planning of Work (Methodology)

The methodology of the project consists of the following steps:

Step 1: Collection of Raw Audio Data

The first step is to collect source recordings containing the target speaker. These may include podcasts, interviews, or conversational recordings. In addition, a short clean reference clip of the target speaker is collected. This reference is necessary for speaker verification.

Inputs include:

long multi-speaker recordings,

short clean single-speaker reference samples.


Step 2: Audio Preprocessing

The collected audio is preprocessed using ffmpeg. Two versions of the audio are created:

a native-quality version preserved for final export,

a 16 kHz mono processing version used for diarization and verification models.


This step ensures compatibility with speaker-processing pipelines while preserving final audio quality for dataset export.

Step 3: Speaker Diarization

The preprocessed audio is passed into a speaker diarization model. In the implemented system, NeMo diarization is used to segment the recording into speaker-labeled intervals.

The output is stored in RTTM format, which contains timestamped speech segments for speakers such as speaker_0, speaker_1, and so on.

At this stage, the system knows when each speaker is active, but it does not yet know which diarized label corresponds to the target person.

Step 4: Reference-Based Speaker Identification

A clean reference clip of the target speaker is encoded into a speaker embedding using the Titanet speaker verification model.

Then, multiple segments from each diarized speaker label are embedded and compared against the reference embedding using cosine similarity.

The diarized label with the highest similarity score is selected as the target speaker. Statistical values such as:

mean similarity,

median similarity,

percentile ranges,

minimum and maximum scores


are used to validate the selection.

Step 5: Overlap Removal

To maintain high dataset purity, the pipeline removes all regions where the target speaker overlaps with other speakers.

This is done by:

collecting all target-speaker intervals,

collecting all non-target-speaker intervals,

subtracting all overlaps.


The result is a set of candidate intervals that contain only clean single-speaker speech.

This step is important because overlapping speech greatly reduces the quality of a voice-cloning dataset.

Step 6: Speaker Verification Filtering

Even after diarization and overlap removal, some segments may still be incorrectly assigned. To solve this, speaker verification filtering is applied.

Two approaches are considered:

a) Per-Interval Verification
Each segment is checked independently against the reference speaker. Segments with similarity below a chosen threshold are moved to quarantine.

b) Super-Segment Verification
Nearby target segments are merged into larger windows before verification. This reduces embedding noise and provides more stable filtering decisions.

The accepted segments form the main training dataset, while suspicious segments are separated into a quarantine set for later inspection.

Step 7: Audit Artifact Generation

To ensure transparency and quality control, the system generates audit artifacts such as:

diarization outputs,

similarity statistics,

kept and quarantined segment logs,

similarity bucket reports,

sample audio clips for manual QA.


This makes the entire process reproducible and inspectable.

Step 8: Export of Clean Training Dataset

After filtering, the final accepted speech intervals are concatenated or exported into clean training-ready audio files.

The final bundle may include:

target_train.wav,

target_quarantine.wav,

diarization.rttm,

label_scores.json,

kept_segments.jsonl,

quarantine_segments.jsonl,

audit_wavs.


This dataset can then be directly used for training or fine-tuning voice-cloning systems.

Step 9: Voice-Cloning Model Preparation

The cleaned dataset is prepared for use in models such as:

GPT-SoVITS,

CosyVoice 3.


Depending on the model, the next steps may include:

segmentation into short utterances,

transcription,

metadata generation,

prompt or style annotation,

speaker adaptation or fine-tuning.


For few-shot experiments, datasets around 17 to 30 minutes are considered practical. For zero-shot systems such as CosyVoice 3, short reference prompts are sufficient.

Step 10: Comparative Evaluation

The project then compares the practical behavior of:

CosyVoice 3 zero-shot cloning, and

fine-tuned GPT-SoVITS / speaker-adapted systems.


Evaluation focuses on:

naturalness,

speaker similarity,

intelligibility,

robustness,

dataset effort versus quality gain.


For roleplay use cases, the key question is whether larger speaker-specific datasets significantly improve emotional and natural delivery, or whether a strong zero-shot base model already performs well enough.

Step 11: Result Analysis

The final analysis studies:

the purity and usable duration of extracted speech,

the effectiveness of diarization plus verification,

the role of overlap removal,

the computational efficiency of the pipeline,

the practical tradeoff between zero-shot and fine-tuned cloning.


Based on the discussion in this project, a likely conclusion is that CosyVoice 3 zero-shot provides strong quality with much lower engineering effort, while fine-tuning gives only marginal gains in many cases and introduces regression risks, especially when the dataset is limited or not perfectly controlled.


---

VI. Bibliography / References

1. Vaswani, A. et al. (2017). Attention Is All You Need. Advances in Neural Information Processing Systems.


2. NVIDIA NeMo Documentation and speaker diarization resources.


3. PyAnnote Audio Documentation and diarization framework resources.


4. PyTorch Documentation.


5. ffmpeg Documentation.


6. GPT-SoVITS open-source repository and related implementation resources.


7. CosyVoice / CosyVoice 2 / CosyVoice 3 technical reports and model documentation.


8. Titanet speaker verification model resources from NVIDIA.


9. Brown, T. et al. (2020). Language Models are Few-Shot Learners.


10. Goodfellow, I., Bengio, Y., & Courville, A. (2016). Deep Learning. MIT Press.


11. NVIDIA CUDA and GPU acceleration documentation.

