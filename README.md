# training-asr-nxt

Script for the preparation of a corpus


## Installation
1. Creare a Python env: `conda create --name tasr python=3.12`
2. Install requirements: `make install/req`
3. Install additional packages (pip somehow do not work): `pip install --cache-dir ..... torchaudio torch speechbrain pyannote.audio faiss-cpu`

## Flow

```mermaid
flowchart TD
    A[Initial corpus, dir/dir/audio.mp4] -->|Convert to 16kHz wav
    Takes: dir/dir/audio.mp4
    Scripts: egs/to_wav
    Cmd: make run/runner| B[Result: Target dir, file: audio.mp4/audio.16.wav
    ]
    B -->|Detect music/speech
    Takes: audio.16.wav
    Scripts: egs/sm_detection_ina
    Cmd: make run/runner| C[Result: Target dir, file: audio.ina_segments
    ]
    C -->|Diarization 
    Takes: audio.16.wav, audio.ina_segments
    Scripts: egs/diarization
    Cmd: make run/runner| D[Result: Target dir, files: audio.rttm, speech.rttm
    ]
    D -->|Speaker clustering across files 
    Takes: audio.16.wav, audio.rttm
    Scripts: egs/speaker_embeddings
    Cmd: make run/runner
    make cluster
    | E[Result: .data/clustered.jsonl
    ]
    D -->|Language detection for speaker 
    Takes: audio.16.wav, audio.rttm
    Scripts: egs/id_lang
    Cmd: make run/runner/speaker
    | F[Result: Target dir, files: lang_detect_speaker.jsonl
    ]
```
