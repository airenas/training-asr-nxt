import argparse
import hashlib
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def update_global_speaker(conn, data, name):
    d_it = data
    if len(data) > 500:
        d_it = tqdm(data)
    with conn.cursor() as cur:
        for d in d_it:
            file_id = d["f"]
            speaker = d["sp"]
            cur.execute(
                "UPDATE file_speakers SET global_speaker = %s WHERE file_id = %s and speaker = %s",
                (name, file_id, speaker)
            )


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Cluster speakers in files, update file_speakers table")
    parser.add_argument("--clusters", nargs='?', required=True, help="Path to clusters jsonl file")
    args = parser.parse_args(args=argv)

    logger.info(f"Input file cluster  : {args.clusters}")
    count = 0
    with open(args.clusters, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            count += 1
    logger.info(f"total clusters: {count}")

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:
        conn.autocommit = False
        with open(args.clusters, "r", encoding="utf-8") as f:
            for i, line in enumerate(tqdm(f, desc="Processing clusters", total=count)):
                if not line.strip():
                    continue
                data = json.loads(line)
                name = "spk_" + hashlib.md5(line.encode()).hexdigest()[:6]
                update_global_speaker(conn, data, name)
                if i % 10 == 0:
                    conn.commit()

        conn.commit()

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
