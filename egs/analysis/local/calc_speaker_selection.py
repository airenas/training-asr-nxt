import argparse
import json
import random
import sys
from collections import defaultdict

from tqdm import tqdm

from egs.analysis.local.prepare_speaker_rttm import FinalSegment
from preparation.logger import logger


def save(f, id, source, res, file_speakers):
    for s in res:
        fs = file_speakers.get(id, {}).get(s.speaker, {})
        obj = {
            "source": source,
            "id": id,
            "speaker": s.speaker,
            "gs": fs.get("global_speaker", ""),
            "gender": fs.get("gender", ""),
            "lang": fs.get("language", ""),
            "start": round(s.start, 3),
            "end": round(s.end, 3),
        }
        f.write(json.dumps(obj) + "\n")


def skip_less_than_in_file(segments, secs):
    totals = defaultdict(float)
    for s in segments:
        totals[s.id] += (s.end - s.start)

    res = []
    for s in segments:
        if totals[s.id] >= secs:
            res.append(s)
    return res


def duration_aug(param, source):
    if source == "crawl":
        return param * 4.0
    return param


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Prepares speaker selection for final training data")
    parser.add_argument("--input", nargs='?', required=True, help="Jsonl with complete speakers data")
    parser.add_argument("--output", nargs='?', required=True, help="File segments to output for training")
    parser.add_argument("--take-up", nargs='?', default=3600, type=float,
                        help="Max duration to take per speaker in seconds")
    parser.add_argument("--dry-run", nargs='?', default=1, type=int,
                        help="If 1, just calc info")

    args = parser.parse_args(args=argv)

    logger.info(f"Input         : {args.input}")
    logger.info(f"Output        : {args.output}")
    logger.info(f"Dry run       : {args.dry_run}")
    logger.info(f"Take up       : {args.take_up} seconds per speaker")

    logger.info("iterate starting")

    items = {}
    skip = 0
    with open(args.input, "r", encoding="utf-8") as f:
        for i, line in enumerate(tqdm(f)):
            # if i > 1000000:
            #     break
            data_dict = json.loads(line)
            segment = FinalSegment(**data_dict)

            if not segment.gender:
                skip += 1
                continue
            if segment.source == "liepa3-speaker":
                segment.source = "liepa3"
            if segment.lang == "" or segment.lang == "lt":
                items.setdefault(segment.gs, []).append(segment)
            else:
                skip += 1
    logger.info(f"loaded {len(items)} global speakers, skipped {skip} segments with language/gender")

    stats = defaultdict(lambda: {
        "duration": 0.0,
        "segments": 0,
        "speakers": set(),
    })

    for gs, segments in tqdm(items.items()):
        segments = skip_less_than_in_file(segments=segments, secs=3)
        random.shuffle(segments)
        duration = 0.0
        selected = []
        for seg in segments:
            dur = seg.end - seg.start
            if duration + dur <= args.take_up:
                selected.append(seg)
                duration += dur
            else:
                break
        for seg in selected:
            key = (seg.source, seg.gender)
            stats[key]["duration"] += seg.end - seg.start
            stats[key]["segments"] += 1
            stats[key]["speakers"].add(seg.gs)

    total_duration = sum(s["duration"] for s in stats.values())
    total_segments = sum(s["segments"] for s in stats.values())
    total_speakers = sum(len(s["speakers"]) for s in stats.values())

    logger.info(f"Total h        : {total_duration / 3600:.2f}")
    logger.info(f"Total segments : {total_segments}")
    logger.info(f"Total speakers : {total_speakers}")

    res = []
    for (source, gender), s in stats.items():
        res.append(
            f"{source:<10} {gender:<5}: "
            f"segments={s['segments']:>8} "
            f"speakers={len(s['speakers']):>8} "
            f"hours={s['duration'] / 3600:>8.2f} "
            f"hours with aug={duration_aug(s['duration'], source) / 3600:>8.2f}"
        )
    res = sorted(res, key=lambda s: s.split(":")[0])
    logger.info("By source gender:")
    for r in res:
        logger.info(r)

    gender_stats = defaultdict(lambda: {
        "duration": 0.0,
        "duration_aug": 0.0,
        "segments": 0,
        "speakers": set(),
    })

    for (source, gender), s in stats.items():
        gender = gender
        gender_stats[gender]["duration"] += s["duration"]
        gender_stats[gender]["duration_aug"] += duration_aug(s["duration"], source)
        gender_stats[gender]["segments"] += s["segments"]
        gender_stats[gender]["speakers"].update(s["speakers"])

    res = []
    for (gender), s in gender_stats.items():
        res.append(
            f"{gender:<5}: "
            f"segments={s['segments']:>8} "
            f"speakers={len(s['speakers']):>8} "
            f"hours={s['duration'] / 3600:>8.2f} "
            f"hours with aug={s['duration_aug'] / 3600:>8.2f}"
        )
    res = sorted(res, key=lambda s: s.split(":")[0])
    logger.info("By gender:")
    for r in res:
        logger.info(r)

    total_duration = sum(s["duration_aug"] for s in gender_stats.values())
    total_segments = sum(s["segments"] for s in gender_stats.values())
    total_speakers = sum(len(s["speakers"]) for s in gender_stats.values())

    logger.info(f"Total h        : {total_duration / 3600:.2f}")
    logger.info(f"Total segments : {total_segments}")
    logger.info(f"Total speakers : {total_speakers}")

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
