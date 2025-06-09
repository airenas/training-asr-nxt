import argparse
import json
import sys

from preparation.logger import logger
from preparation.utils.language import detect_language, SpeakerLangRes
from preparation.utils.rttm import load_rttm, remove_overlaps, take_middle
from preparation.utils.sm_segments import Segment


def get_speaker_segments(annotation,
                         speaker: str,
                         count: int = 5,
                         min_segment_duration: float = 5) -> list[Segment]:
    segments = [Segment(start=s.start, end=s.end, label=speaker) for s, _, label in
                annotation.itertracks(yield_label=True) if
                label == speaker]

    segments = [s for s in segments if s.duration >= min_segment_duration]
    segments.sort(key=lambda x: x.duration, reverse=True)
    res = []
    for s in segments:
        res.append(take_middle(s, min_segment_duration))
        if len(res) >= count:
            break
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Detect language for speakers")
    parser.add_argument("--input", nargs='?', required=True, help="Audio file, must contains audio.rttm nearby")
    parser.add_argument("--output", nargs='?', required=True,
                        help="Output file in jsonl; each line is a json object with segment, lang and conf")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")

    annotations = load_rttm(args.input)
    annotations = remove_overlaps(annotations)
    res = []
    if len(annotations) > 0:
        logger.info(f"loaded rttm lines: {len(annotations)}")
        speakers = set(annotations.labels())
        logger.info(f"loaded speakers: {len(speakers)}")
        for sp in speakers:
            logger.info(f"Speaker: {sp}")
            for sl in [5, 3, 1]:
                test_segments = get_speaker_segments(annotations, sp, count=5, min_segment_duration=sl)
                if len(test_segments) > 1:
                    break
            if len(test_segments) > 0:
                logger.info(f"Made test segments: {test_segments}")
                speaker_lang = detect_language(args.input, test_segments)
                logger.info(f"Got results: {speaker_lang}")
                res.append(SpeakerLangRes(speaker=sp, lang_res=speaker_lang))

    with open(args.output, "w") as f:
        for r in res:
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")
    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
