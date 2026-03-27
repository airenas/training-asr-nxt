import argparse
import json
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def make_rttm(parsed):
    res = ""
    for segment in parsed:
        start = segment["from"]
        to = segment["to"]
        res += f"SPEAKER waveform 1 {start:.3f} {to - start:.3f} <NA> <NA> SPEAKER_00 <NA> <NA>\n"
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make audio.rttm from vad segments")
    parser.add_argument("--source", nargs='?', required=True, help="Source")
    parser.add_argument("--dry-run", nargs='?', default=1, type=int,
                        help="If 1, do not actually create, just print what would be done")

    args = parser.parse_args(args=argv)

    logger.info(f"Source        : {args.source}")
    logger.info(f"Dry run       : {args.dry_run}")

    db_url = os.environ["DB_URL"]
    logger.info("iterate starting")
    conn = psycopg2.connect(dsn=db_url)
    BATCH_SIZE = 1000
    batch_count = 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM files f INNER JOIN kv on f.id = kv.id where kv.type='vad.json' and f.source = %s",
            (args.source,)
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No source with name {args.source}")
        count = row[0]
        logger.info(f"Source {args.source} has {count} files")

    processed, skipped = 0, 0

    read_cur = conn.cursor(name="read_cursor", withhold=True)
    write_cur = conn.cursor()
    try:
        read_cur.itersize = 100
        read_cur.execute(
            "SELECT kv.id, kv.content FROM files f INNER JOIN kv on f.id = kv.id where kv.type='vad.json' and f.source = %s",
            (args.source,)
        )
        for row in tqdm(read_cur, total=count, desc="Processing"):
            id, content = row[0], bytes(row[1])
            parsed = json.loads(content.decode("utf-8"))
            rttm = make_rttm(parsed)
            if not args.dry_run == 1:
                write_cur.execute(
                    "INSERT INTO kv(id, type, content) VALUES(%s, %s, %s)",
                    (id, "audio.rttm", rttm.encode("utf-8"))
                )
                batch_count += 1

                if batch_count >= BATCH_SIZE:
                    conn.commit()
                    batch_count = 0

            # final commit
        if not args.dry_run == 1:
            conn.commit()

        logger.info(f"Processed {processed} files, skipped {skipped}")
    finally:
        read_cur.close()
        write_cur.close()
        conn.close()

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
