import argparse
import os
import sys

import psycopg2
from tqdm import tqdm

from egs.analysis.local.speaker import load_rttm
from preparation.logger import logger


def read_files_table(conn, sources):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM files WHERE source = ANY(%s)", (sources,))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def calculate_speaker_durations(rttm):
    speaker_durations = {}
    for speaker, segments in rttm.items():
        total_duration = sum(seg["to"] - seg["from"] for seg in segments)
        speaker_durations[speaker] = (total_duration, len(segments))
    return speaker_durations


def upsert_file_speaker(conn, file_id: str, speaker_id: str, duration: float, segments: int):
    sql = """
    INSERT INTO file_speakers (file_id, speaker, duration_in_sec, segments)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (file_id, speaker)
    DO UPDATE SET duration_in_sec = EXCLUDED.duration_in_sec, segments = EXCLUDED.segments
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_id, speaker_id, duration, segments))


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Starting prepare speaker file table")
    parser.add_argument("--source", action='append', required=True, help="Input source(s)")
    args = parser.parse_args(args=argv)

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:
        conn.autocommit = False

        logger.info(f"Reading file ids")
        files = read_files_table(conn, args.source)
        logger.info(f"Got {len(files)} files")
        for idx, file_id in enumerate(tqdm(files, desc="Processing files")):
            if idx % 1000 == 0:
                conn.commit()
            rttm = load_rttm(conn, file_id)
            durations = calculate_speaker_durations(rttm)
            for speaker_id, (duration, l_segments) in durations.items():
                upsert_file_speaker(conn, file_id, speaker_id, duration, l_segments)
        conn.commit()

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
