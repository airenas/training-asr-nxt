import argparse
import json
import os
import queue
import sys
import threading
from dataclasses import dataclass, asdict
from typing import List

import psycopg2
from tqdm import tqdm

from preparation.logger import logger
from preparation.utils.sm_segments import parse_segments, SegmentLabel

MIN_SEC_IN_FILE = 3

MUSIC = SegmentLabel.MUSIC.value


class CalcData:
    def __init__(self):
        self.id = ""
        self.source = ""
        self.duration = 0.0
        self.rttm = None
        self.ina_segments = None


class Segment:
    def __init__(self, speaker: str, start: float, end: float):
        self.speaker = speaker
        self.start = start
        self.end = end


@dataclass
class FinalSegment:
    source: str
    id: str
    speaker: str
    gs: str  # global speaker
    gender: str
    lang: str
    start: float
    end: float


def parse_rttm_to_list(rttm_bytes: bytes):
    res = []
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
        res.append(Segment(speaker=spk, start=start, end=end))
    return res


def parse_ina_to_list(ina_segments):
    seg = parse_segments(str(ina_segments))
    res = []
    for s in seg:
        if s.label == MUSIC:
            res.append(Segment(speaker=MUSIC, start=s.start, end=s.end))
    return res


def calc_gap(end, start, extend_end: bool = False, extend_start: bool = False):
    gap = start - end
    extend = min(gap / 2, 0.5)
    if extend_end:
        end += extend
    if extend_start:
        start -= extend
    return end, start


def prepare_segments(seg: List[Segment], duration: float) -> List[Segment]:
    res = []
    seg.sort(key=lambda s: s.start)
    at = 0;
    for i in range(len(seg)):
        cur = seg[i]
        if cur.speaker == MUSIC:
            at = cur.end
            continue
        if cur.end < at:
            continue
        if cur.start < at:
            cur.start = at
        if i == 0:
            _, cur.start = calc_gap(at, cur.start, False, cur.speaker != MUSIC)

        if i == len(seg) - 1:
            cur.end, _ = calc_gap(cur.end, duration, True, False)
            res.append(Segment(speaker=cur.speaker, start=cur.start, end=cur.end))
            continue

        next = seg[i + 1]
        if cur.end <= next.start:
            cur.end, next.start = calc_gap(cur.end, next.start, True, next.speaker != MUSIC)
            at = cur.end
            res.append(Segment(speaker=cur.speaker, start=cur.start, end=cur.end))
        else:
            at = cur.end
            cur.end = next.start
            res.append(Segment(speaker=cur.speaker, start=cur.start, end=cur.end))
    return res


def join_same(seg: List[Segment]) -> List[Segment]:
    res = []
    for s in seg:
        prev: Segment | None = None
        if len(res) > 0:
            prev = res[-1]
        if not prev:
            res.append(s)
            continue
        if s.speaker == prev.speaker and s.start - prev.end < 0.2:
            prev.end = s.end
            continue
        res.append(s)
    return res


def calc(d: CalcData):
    rttm = parse_rttm_to_list(d.rttm)
    if d.source in ['liepa3', 'liepa3-speaker']:
        if len(rttm) > 0:
            rttm[0].start = 0
            rttm[0].end = d.duration
            return [rttm[0]]
    if d.ina_segments:
        ina = parse_ina_to_list(d.ina_segments)
        rttm.extend(ina)
    return join_same(seg=prepare_segments(rttm, d.duration))


def save(f, id, source, res, file_speakers):
    for s in res:
        fs = file_speakers.get(id, {}).get(s.speaker, {})
        final = FinalSegment(
            source=source,
            id=id,
            speaker=s.speaker,
            gs=fs.get("global_speaker", ""),
            gender=fs.get("gender", ""),
            lang=fs.get("language", ""),
            start=round(s.start, 3),
            end=round(s.end, 3),
        )
        f.write(json.dumps(asdict(final)) + "\n")


def load_file_speakers(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM file_speakers",
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No file speakers")
        count = row[0]
        logger.info(f"File_speakers: {count}")
        file_speakers = {}
        cur.execute(
            "SELECT file_id, speaker, global_speaker, gender, language FROM file_speakers",
        )
        for row in tqdm(cur, total=count, desc="Loading file speakers"):
            file_id, speaker, global_speaker, gender, language = row[0], row[1], row[2], row[3], row[4]
            if language:
                language = language.strip()
            else:
                language = ""
            file_speakers.setdefault(file_id, {})[speaker] = {
                "global_speaker": global_speaker,
                "gender": gender,
                "language": language,
            }
        return file_speakers


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make train data from db")
    parser.add_argument("--output", nargs='?', required=True, help="File segments to output for training")
    parser.add_argument("--dry-run", nargs='?', default=1, type=int,
                        help="If 1, just calc info")

    args = parser.parse_args(args=argv)

    logger.info(f"Output        : {args.output}")
    logger.info(f"Dry run       : {args.dry_run}")

    db_url = os.environ["DB_URL"]
    logger.info("iterate starting")

    write_queue = queue.Queue(maxsize=100)  # queue for passing data to writer
    stop_signal = object()  # sentinel to tell writer to stop

    file_speakers = None

    # Writer thread function
    def writer_thread(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            while True:
                item = write_queue.get()
                try:
                    if item is stop_signal:
                        break
                    if not args.dry_run:
                        _id, source, res = item
                        save(f, _id, source, res, file_speakers)
                finally:
                    write_queue.task_done()

    thread = threading.Thread(target=writer_thread, args=(args.output,))
    thread.start()

    try:
        with psycopg2.connect(dsn=db_url) as conn:
            file_speakers = load_file_speakers(conn)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM kv where type = 'audio.rttm' or type = 'audio.ina_segments'",
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"No rttms")
                count = row[0]
                logger.info(f"RTTMs: {count}")

            processed, extracted = 0, 0

            data = CalcData()

            read_cur = conn.cursor(name="read_cursor")
            try:
                read_cur.itersize = 100
                read_cur.execute(
                    "SELECT kv.id, kv.type, kv.content, f.duration_in_sec, f.source FROM kv left join files f on kv.id = f.id where kv.type = 'audio.rttm' or kv.type = 'audio.ina_segments' order by kv.id",
                )
                for row in tqdm(read_cur, total=count, desc="Processing"):
                    f_id, kv_type, content, duration, source = row[0], row[1], bytes(row[2]), row[3], row[4]
                    if source == "liepa3-old":
                        continue
                    if data.id != f_id and data.id != "":
                        res = calc(data)
                        write_queue.put((data.id, data.source, res))
                        extracted += len(res)
                        data = CalcData()
                    data.id = f_id
                    data.source = source
                    data.duration = duration
                    if kv_type == "audio.rttm":
                        data.rttm = content
                    elif kv_type == "audio.ina_segments":
                        data.ina_segments = content
                    else:
                        raise ValueError(f"Unknown kv type {kv_type}")
                    processed += 1
                res = calc(data)
                write_queue.put((data.id, data.source, res))
                extracted += len(res)
                logger.info(f"Processed {processed} files, parts {extracted}")
            finally:
                read_cur.close()
    finally:
        write_queue.put(stop_signal)
        logger.info(f"wait for writer to finish")
        thread.join()

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
