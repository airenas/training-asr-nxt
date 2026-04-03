import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import List

import psycopg2
from tqdm import tqdm

from egs.analysis.local.prepare_speaker_rttm import FinalSegment
from preparation.logger import logger

@dataclass
class Segment:
    start: float
    end: float

@dataclass
class FileCuts:
    source: str
    path: str
    segments: List[Segment]


def load_files(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM files",
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No files")
        count = row[0]
        logger.info(f"Files: {count}")
        cur.execute(
            "SELECT id, path FROM files",
        )
        res = {}
        for row in tqdm(cur, total=count, desc="Loading file"):
            file_id, path = row[0], row[1]
            res[file_id] = path
        return res


def save(f, file_data: FileCuts):
    f.write(json.dumps(asdict(file_data)) + "\n")


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Prepares file cuts")
    parser.add_argument("--input", nargs='?', required=True, help="Jsonl with complete segments")
    parser.add_argument("--output", nargs='?', required=True, help="File segments to cut audio")
    parser.add_argument("--dry-run", nargs='?', default=1, type=int,
                        help="If 1, just calc info")

    args = parser.parse_args(args=argv)

    logger.info(f"Input         : {args.input}")
    logger.info(f"Output        : {args.output}")
    logger.info(f"Dry run       : {args.dry_run}")

    db_url = os.environ["DB_URL"]
    logger.info("load files")
    with psycopg2.connect(dsn=db_url) as conn:
        files = load_files(conn)
    logger.info(f"Loaded)   {len(files)} files")
    logger.info("Load cut segments")
    items = []
    with open(args.input, "r", encoding="utf-8") as f:
        for i, line in enumerate(tqdm(f)):
            data_dict = json.loads(line)
            segment = FinalSegment(**data_dict)
            items.append(segment)
    logger.info(f"loaded {len(items)} segments")
    logger.info(f"loaded {len(items)} sorting")
    items.sort(key=lambda s: (s.id, s.start), )

    last_id = None
    curr_file = FileCuts(path="", source="", segments=[])
    with open(args.output, "w", encoding="utf-8") as output_f:
        for seg in tqdm(items):
            if last_id != seg.id:
                if len(curr_file.segments) and args.dry_run == 0:
                    save(f=output_f, file_data=curr_file)
                last_id = seg.id
                file_path = files.get(seg.id)
                if not file_path:
                    logger.warning(f"File id {seg.id} not found in db, skipping")
                    continue
                curr_file = FileCuts(path=file_path, source=seg.source, segments=[])
            curr_file.segments.append(Segment(start=seg.start, end=seg.end))
        if len(curr_file.segments) and args.dry_run == 0:
            save(f=output_f, file_data=curr_file)

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
