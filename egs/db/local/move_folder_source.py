import argparse
import os
import sys

import psycopg2
from tqdm import tqdm

from preparation.logger import logger


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Move top n random wavs from catalog to another source")
    parser.add_argument("--input-source", nargs='?', required=True, help="Input source")
    parser.add_argument("--target-source", nargs='?', required=True, help="Target source")
    parser.add_argument("--n", nargs='?', default=5, type=int,
                        help="How many wavs to move to new source")
    parser.add_argument("--dry-run", nargs='?', default=1, type=int,
                        help="If 1, do not actually move files, just print what would be moved")

    args = parser.parse_args(args=argv)

    logger.info(f"Input source  : {args.input_source}")
    logger.info(f"Target source : {args.target_source}")
    logger.info(f"N             : {args.n}")

    db_url = os.environ["DB_URL"]
    logger.info("iterate starting")
    conn = psycopg2.connect(dsn=db_url)
    BATCH_SIZE = 1000
    batch_count = 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM files WHERE source = %s",
            (args.input_source,)
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No source with name {args.input_source}")
        count = row[0]
        logger.info(f"Source {args.input_source} has {count} files")

    processed, moved, speakers = 0, 0, 0
    ids = []
    with conn.cursor(name="read_cursor", withhold=True) as cur:
        cur.itersize = 1000
        cur.execute(
            "SELECT id, path FROM files WHERE source = %s ORDER BY path",
            (args.input_source,)
        )
        speaker_moved = 0
        last_path = None
        for row in tqdm(cur, total=count):
            id = row[0]
            full_path = row[1]
            path = os.path.dirname(full_path)
            processed += 1
            if path == last_path and speaker_moved >= args.n:
                continue

            # move file to target source
            ids.append(id)
            if path != last_path:
                speaker_moved = 1
                last_path = path
                speakers += 1
            else:
                speaker_moved += 1
    logger.info(f"will update {len(ids)}")
    logger.info(f"Processed {processed} files, will move {len(ids)} files to source {args.target_source}")
    logger.info(f"Speakers {speakers}")

    if not args.dry_run == 1:
        with conn.cursor() as cur:
            for id in tqdm(ids, desc="Updating"):
                cur.execute(
                    "UPDATE files SET source = %s WHERE id = %s",
                    (args.target_source, id)
                )
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    conn.commit()
                    batch_count = 0
                moved += 1

        conn.commit()
        logger.info(f"Moved {moved}")
    else:
        logger.info(f"Dry run, no files moved")

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
