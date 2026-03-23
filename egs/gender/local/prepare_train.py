import argparse
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger
from preparation.metadata.embedding import get_speaker_embedding


def find_labeled(data, labels):
    for item in data:
        key = (item["f"], item["sp"])
        res = labels.get(key)
        if res:
            return res
    return None


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Label audio to gender")
    parser.add_argument("--input-gender", nargs='?', required=True, help="Gender file")
    parser.add_argument("--input-cluster", nargs='?', required=True, help="Cluster file")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file gender   : {args.input_gender}")
    logger.info(f"Input file cluster  : {args.input_cluster}")
    logger.info(f"Output file         : {args.output}")

    labels = {}
    with open(args.input_gender, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            key = (data["f"], data["sp"])
            labels[key] = data
    logger.info(f"loaded {len(labels)} labels")
    found, skipped = 0, 0
    db_url = os.environ["DB_URL"]
    with psycopg2.connect(dsn=db_url) as conn:
        with open(args.input_cluster, "r", encoding="utf-8") as f:
            with open(args.output, "a", encoding="utf-8") as output_f:
                for i, line in enumerate(f):
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    logger.info(f"{i}: got {len(data)} records")

                    li = find_labeled(data, labels)
                    if not li:
                        logger.info(f"{i}: not found labels, skipping")
                        skipped += 1
                        continue
                    found += 1
                    ed = {}
                    l = li['label']
                    if l != "m" and l != "f":
                        logger.info(f"{i}: label is not m or f, skipping")
                        skipped += 1
                        continue

                    for sp_i, item in enumerate(tqdm(data, desc=f"Processing {i}")):
                        if sp_i > 200:
                            logger.info(f"{i}: more than 200 speaker emb, skipping")
                            break
                        emb = get_speaker_embedding(conn, item["f"], item["sp"])
                        ed['emb'] = emb
                        ed['g'] = l
                        output_f.write(json.dumps(ed) + "\n")

    logger.info(f"found {found}, skipped {skipped}")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
