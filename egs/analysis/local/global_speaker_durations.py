import argparse
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def read_file_speakers(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT file_id, speaker_id, language, duration_in_sec FROM file_speakers")
        rows = cur.fetchall()
    return rows


def prepare_data(file_speaker_rows):
    res = {}
    for (file_id, speaker_id, language, duration) in tqdm(file_speaker_rows, desc="Processing file speakers"):
        res.setdefault(file_id, {}).setdefault(speaker_id, (language, duration))
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Starting prepare user segments")
    parser.add_argument("--input", nargs='?', required=True, help="jsonl of clustered speakers")
    parser.add_argument("--output", nargs='?', required=True,
                        help="Output file in jsonl (speaker, duration)")
    args = parser.parse_args(args=argv)

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:
        logger.info(f"Reading file speakers")
        file_speaker_rows = read_file_speakers(conn)
        file_speakers = prepare_data(file_speaker_rows)

    logger.info(f"Got {len(file_speakers)} files with speakers")

    loaded_speakers = []
    with open(args.input, "r") as f:
        for line in f:
            d = json.loads(line)
            loaded_speakers.append(d)
    logger.info(f"loaded {len(loaded_speakers)} clustered speakers")
    res = []
    for idx, speaker in enumerate(tqdm(loaded_speakers, desc="Calculating")):
        duration_sum = 0.0
        duration_non_lt = 0.0
        for sp_file in speaker:
            (lang, duration) = file_speakers.get(sp_file["f"], {}).get(sp_file["sp"], ("", 0.0))
            if duration == 0.0:
                logger.warning(f"No duration for file {sp_file['f']} speaker {sp_file['sp']}")
            if lang == "":
                logger.warning(f"No lang for file {sp_file['f']} speaker {sp_file['sp']}")
            if lang == 'lt':
                duration_sum += duration
            else:
                duration_non_lt += duration
        res.append({"sp": f"sp_{idx:05d}", "duration": duration_sum, "duration_non_lt": duration_non_lt})

    # res = sorted(res, key=lambda x: x["duration"], reverse=True)
    with open(args.output, "w") as f:
        for r in res:
            f.write(json.dumps(r) + "\n")
    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
