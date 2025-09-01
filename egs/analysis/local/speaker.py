import argparse
import json
import os
import sys
from typing import List, Dict

import psycopg2

from preparation.logger import logger


def parse_rttm_bytes(rttm_bytes: bytes):
    speaker_segments: Dict[str, List[dict]] = {}
    text = bytes(rttm_bytes).decode("utf-8")

    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        # SPEAKER <file_id> 1 <start> <duration> <ortho> <stype> <name> <conf>
        parts = line.split()
        if len(parts) < 8:
            continue
        spk = parts[7]
        start = float(parts[3])
        duration = float(parts[4])
        end = start + duration
        speaker_segments.setdefault(spk, []).append({
            "from": start,
            "to": end,
        })

    return speaker_segments


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Starting prepare user segments")
    parser.add_argument("--input", nargs='?', required=True, help="json list of files and speakers")
    parser.add_argument("--output", nargs='?', required=True,
                        help="Output file in json")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file  : {args.input}")
    logger.info(f"Output file  : {args.output}")

    logger.info(f"reading  file {args.input}")
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"got {len(data)} records")
    file_ids = [item["f"] for item in data]

    DB_URL = os.environ["DB_URL"]
    result = []
    duration_sum = 0.0
    with psycopg2.connect(dsn=DB_URL) as conn:
        for item in data:
            f = item.get("f")
            speaker = item.get("sp")
            logger.info(f"Processing file id {f} speaker {speaker}")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, path FROM files WHERE id = %s",
                    (f,)
                )
                row = cur.fetchone()
            if not row:
                raise ValueError(f"No file id {f}")
            path = row[1]
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT content FROM kv WHERE id = %s AND type = 'audio.rttm'",
                    (f,)
                )
                rttm_row = cur.fetchone()
            if rttm_row:
                rttm_bytes = rttm_row[0]
                rttm = parse_rttm_bytes(bytes(rttm_bytes))
            else:
                raise ValueError(f"No RTTM found for file id {f}")
            segments = rttm.get(speaker, [])
            if not segments:
                raise ValueError(f"No segment for file id {f} speaker {speaker}")
            duration_f = 0
            for s in segments:
                entry = {
                    "file": f"{path}/audio.16.wav",
                    "from": s["from"],
                    "to": s["to"],
                }
                duration_f += s["to"] - s["from"]
                result.append(entry)
            logger.info(f"{path} -> {len(segments)} segments, {duration_f:.2f}s")
            duration_sum += duration_f


    with open(args.output, "w") as f:
        f.write(json.dumps(result, ensure_ascii=False))
    logger.info(f"duration {duration_sum:.2f}s")
    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
