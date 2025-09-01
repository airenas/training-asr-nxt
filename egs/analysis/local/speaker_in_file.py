import argparse
import os
import sys

import psycopg2
from tqdm import tqdm

from egs.analysis.local.speaker import load_rttm
from preparation.logger import logger


def read_files_table(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM files")
        rows = cur.fetchall()
    return [row[0] for row in rows]


def calculate_speaker_durations(rttm):
    speaker_durations = {}
    for speaker, segments in rttm.items():
        total_duration = sum(seg["to"] - seg["from"] for seg in segments)
        speaker_durations[speaker] = total_duration
    return speaker_durations


def upsert_file_speaker(conn, file_id: str, speaker_id: str, duration: float):
    sql = """
    INSERT INTO file_speakers (file_id, speaker_id, duration_in_sec)
    VALUES (%s, %s, %s)
    ON CONFLICT (file_id, speaker_id)
    DO UPDATE SET duration_in_sec = EXCLUDED.duration_in_sec
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_id, speaker_id, duration))


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Starting prepare user segments")

    # args = parser.parse_args(args=argv)

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:
        conn.autocommit = True

        logger.info(f"Reading file ids")
        files = read_files_table(conn)
        logger.info(f"Got {len(files)} files")
        for idx, file_id in enumerate(tqdm(files, desc="Processing files")):
            if idx % 100:
                conn.commit()
            rttm = load_rttm(conn, file_id)
            durations = calculate_speaker_durations(rttm)
            for speaker_id, duration in durations.items():
                upsert_file_speaker(conn, file_id, speaker_id, duration)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
