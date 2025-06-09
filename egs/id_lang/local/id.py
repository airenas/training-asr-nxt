import argparse
import json
import sys

from preparation.logger import logger
from preparation.utils.language import detect_language
from preparation.utils.sm_segments import Segment, load_segments, SegmentLabel


def find_segment(speech_segments, start, min_len):
    to = start + min_len
    for s in speech_segments:
        if s.duration < min_len:
            continue
        if s.start >= start:
            return Segment(label=s.label, start=s.start, end=s.start + min_len)
        if s.end >= to:
            return Segment(label=s.label, start=s.end - min_len, end=s.end)
        if s.start < start and s.end > to:
            return Segment(label=s.label, start=start, end=to)
    return None


def select_test_segments(segments, num_segments=5, min_len=5):
    speech_segments = [seg for seg in segments if seg.label == SegmentLabel.SPEECH.value]
    total_duration = sum(s.end - s.start for s in speech_segments)
    logger.info(f"total speech duration: {total_duration}")
    parts_from = total_duration / num_segments + 1
    res = []
    for i in range(num_segments):
        start = i * parts_from
        s = find_segment(speech_segments, start, min_len)
        if s:
            if s not in res:
                res.append(s)
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio processing")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")
    segments = load_segments(args.input)
    # logger.info(f"Got segments: {segments}")
    for sl in [5, 3, 1]:
        test_segments = select_test_segments(segments, num_segments=5, min_len=sl)
        if len(test_segments) > 1:
            break
    res = []
    if len(test_segments) > 0:
        logger.info(f"Made test segments: {test_segments}")
        res = detect_language(args.input, test_segments)
        logger.info(f"Got results: {res}")

    with open(args.output, "w") as f:
        for r in res:
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")
    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
