import argparse
import json
import os
import random
import sys

import psycopg2
from tqdm import tqdm

from egs.gender.local.model import Model
from preparation.logger import logger
from preparation.metadata.embedding import get_speaker_embedding


def find_labeled(data, labels):
    for item in data:
        key = (item["f"], item["sp"])
        res = labels.get(key)
        if res:
            return res
    return None


def take_random(data, count: int):
    return random.sample(data, min(count, len(data)))


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Label audio to gender")
    parser.add_argument("--input-cluster", nargs='?', required=True, help="Cluster file")
    parser.add_argument("--model", nargs='?', required=True, help="Gender xg boost model")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file cluster  : {args.input_cluster}")
    logger.info(f"Input model         : {args.model}")
    logger.info(f"Output file         : {args.output}")

    model = Model(args.model)
    logger.info(f"loaded model")

    total = 0
    with open(args.input_cluster, "r", encoding="utf-8") as f:
        for i, line in enumerate(tqdm(f, desc=f"Preread clusters file")):
            if not line.strip():
                continue
            total += 1
    logger.info(f"total clusters: {total}")


    found, skipped = 0, 0
    db_url = os.environ["DB_URL"]
    with psycopg2.connect(dsn=db_url) as conn:
        with open(args.input_cluster, "r", encoding="utf-8") as f:
            with open(args.output, "w", encoding="utf-8") as output_f:
                for i, line in enumerate(tqdm(f, desc=f"Processing", total=total)):
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    test  = take_random(data, 10)

                    t_res = set()
                    for item in test:
                        emb = get_speaker_embedding(conn, item["f"], item["sp"])
                        label = model.predict(emb=emb, threshold_diff=0.2)
                        output_f.write(json.dumps({
                            "f": item["f"],
                            "sp": item["sp"],
                            "g": label.to_str(),
                        }, ensure_ascii=False) + "\n")
                        t_res.add(label)
                    if len(t_res)  > 1:
                        logger.warn(f"{i}: got different labels {t_res} for same cluster")


    logger.info(f"found {found}, skipped {skipped}")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
