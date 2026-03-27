import argparse
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Move wavs from the same catalog to one cluster")
    parser.add_argument("--source", action='append', required=True, help="Input source(s)")
    parser.add_argument("--output", nargs='?', required=True, help="Output file")

    args = parser.parse_args(args=argv)

    logger.info(f"Input sources  : {args.source}")

    db_url = os.environ["DB_URL"]
    logger.info("iterate starting")
    conn = psycopg2.connect(dsn=db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM files WHERE source = ANY(%s)",
            (args.source,)
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No source with name {args.source}")
        count = row[0]
        logger.info(f"Source {args.source} has {count} files")

    processed, speakers = 0, 0
    ids = []
    with open(args.output, "w") as f:
        with conn.cursor(name="read_cursor", withhold=True) as cur:
            cur.itersize = 1000
            cur.execute(
                "SELECT id, path FROM files WHERE source = ANY(%s) ORDER BY path",
                (args.source,)
            )
            last_path = None
            for row in tqdm(cur, total=count):
                id = row[0]
                full_path = row[1]
                path = os.path.dirname(full_path)
                processed += 1
                if path == last_path:
                    ids.append({"f": id, "sp": "SPEAKER_00"})
                    continue
                speakers += 1
                if len(ids) > 0:
                    f.write(json.dumps(ids) + "\n")
                ids = [{"f": id, "sp": "SPEAKER_00"}]
                last_path = path
        if len(ids) > 0:
            f.write(json.dumps(ids) + "\n")
    logger.info(f"Collected {len(ids)}")
    logger.info(f"Processed {processed}")
    logger.info(f"Speakers {speakers}")
    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
