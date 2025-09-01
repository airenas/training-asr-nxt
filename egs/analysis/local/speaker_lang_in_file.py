import json
import os
import sys
from typing import Dict

import psycopg2
from tqdm import tqdm

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


def upsert_file_speaker(conn, file_id: str, speaker_id: str, language: str):
    sql = """
    INSERT INTO file_speakers (file_id, speaker_id, language)
    VALUES (%s, %s, %s)
    ON CONFLICT (file_id, speaker_id)
    DO UPDATE SET language = EXCLUDED.language
    """
    with conn.cursor() as cur:
        cur.execute(sql, (file_id, speaker_id, language))


def load_lang(conn, file_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content FROM kv WHERE id = %s AND type = 'lang_detect_speaker.jsonl'",
            (file_id,)
        )
        lang_row = cur.fetchone()
    if lang_row:
        lang_bytes = lang_row[0]
        return parse_lang(bytes(lang_bytes))

    raise ValueError(f"No lang found for file id {file_id}")


def get_lang(param) -> str:
    if not param:
        return "unk"
    # 5 and one non lt with low confidence
    lt_count = 0
    non_lt_count = 0
    non_lt_max_conf = 0.0
    non_lt_max_conf_lang = ""
    for (lang, conf) in param:
        if lang == "lt":
            lt_count += 1
        else:
            non_lt_count += 1
            if conf > non_lt_max_conf:
                non_lt_max_conf = conf
                non_lt_max_conf_lang = lang
    if non_lt_count > 1:
        return non_lt_max_conf_lang
    if non_lt_count == 1:
        if lt_count > 3 and non_lt_max_conf < 0.6:
            return "lt"
        else:
            return non_lt_max_conf_lang
    return "lt"


def parse_lang(lang_bytes: bytes):
    res: Dict[str, str] = {}
    text = bytes(lang_bytes).decode("utf-8")

    for line in text.splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        speaker = data["speaker"]
        res[speaker] = get_lang([(d["lang"], d["conf"]) for d in data.get("lang_res", [])])
    return res


def main(argv):
    logger.info("Starting")
    # parser = argparse.ArgumentParser(description="Starting prepare user segments")
    # args = parser.parse_args(args=argv)

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:

        logger.info(f"Reading file ids")
        files = read_files_table(conn)
        logger.info(f"Got {len(files)} files")
        for (idx, file_id) in enumerate(tqdm(files, desc="Processing files")):
            if idx % 100:
                conn.commit()
            langs = load_lang(conn, file_id)
            for speaker_id, lang in langs.items():
                upsert_file_speaker(conn, file_id, speaker_id, lang)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
