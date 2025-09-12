CREATE TABLE IF NOT EXISTS files (
    id VARCHAR(64) NOT NULL,  -- SHA1/MD5 hash of original filename
    path TEXT NOT NULL,       -- Keep the real filename
    duration_in_sec DOUBLE PRECISION, 
    source VARCHAR(20) NOT NULL DEFAULT '', -- e.g., "librispeech", "voxpopuli", etc.
    PRIMARY KEY (id)
);

CREATE INDEX idx_files_source ON files(source);

CREATE TABLE IF NOT EXISTS kv (
    id VARCHAR(64) NOT NULL,      -- SHA1/MD5 hash of original filename
    type TEXT NOT NULL,                  -- "rttm", "segments", "json", "txt", etc.
    content BYTEA NOT NULL,              -- File content (binary safe)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (id, type)
);

CREATE TABLE IF NOT EXISTS file_speakers (
    file_id VARCHAR(64) NOT NULL,     
    speaker_id VARCHAR(64) NOT NULL,   -- ID of the speaker
    duration_in_sec DOUBLE PRECISION, 
    language TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (file_id, speaker_id)
);
