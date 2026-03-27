import argparse
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def update_gender(conn, data, gender):
    d_it = data
    if len(data) > 500:
        d_it = tqdm(data)
    with conn.cursor() as cur:
        for d in d_it:
            file_id = d["f"]
            speaker = d["sp"]
            cur.execute(
                "UPDATE file_speakers SET gender = %s WHERE file_id = %s and speaker = %s",
                (gender, file_id, speaker)
            )


def find_gender(genders, data):
    f, m, u = 0, 0, 0
    for d in data:
        key = (d["f"], d["sp"])
        g = genders.get(key, "")
        if g == "m":
            m += 1
        elif g == "f":
            f += 1
        elif g == "-":
            u += 1
    if m > f and m > u:
        return "m"
    elif f > m and f > u:
        return "f"
    return "-"


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Update gender in file_speakers table")
    parser.add_argument("--clusters", nargs='?', required=True, help="Path to clusters jsonl file")
    parser.add_argument("--genders", nargs='?', required=True, help="Path to genders jsonl file")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file clusters : {args.clusters}")
    logger.info(f"Input file genders  : {args.genders}")
    count = 0
    with open(args.clusters, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            count += 1
    logger.info(f"total clusters: {count}")

    genders = {}
    with open(args.genders, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            key = (data["f"], data["sp"])
            genders[key] = data["g"]

    logger.info(f"Loaded {len(genders)} genders")

    DB_URL = os.environ["DB_URL"]
    with psycopg2.connect(dsn=DB_URL) as conn:
        conn.autocommit = False
        with open(args.clusters, "r", encoding="utf-8") as f:
            for i, line in enumerate(tqdm(f, desc="Processing clusters", total=count)):
                if not line.strip():
                    continue
                data = json.loads(line)
                gender = find_gender(genders, data)
                update_gender(conn, data, gender)
                if i % 10 == 0:
                    conn.commit()

        conn.commit()

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
