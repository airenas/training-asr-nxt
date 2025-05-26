import sys
import argparse
import io
import os
import sys

from preparation.logger import logger
from pyannote.database.util import load_rttm
from textgrid import TextGrid, IntervalTier

from preparation.utils.sm_segments import load_segments


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Converts rttm to textgrid")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")

    dir_name = os.path.dirname(args.input)
    sm_file = os.path.join(dir_name, "audio.ina_segments")
    sm_segments = load_segments(sm_file)
    max_time = max(segment.end for segment in sm_segments) + 0.01


    annotation = load_rttm(args.input).get('waveform')
    tg = TextGrid()

    speaker_segments = {}
    for segment, track, label in annotation.itertracks(yield_label=True):
        speaker_segments.setdefault(label, []).append((segment.start, segment.end))


    for speaker, segments in speaker_segments.items():
        tier = IntervalTier(name=f"Speaker_{speaker}", minTime=0.0, maxTime=max_time)
        for start, end in segments:
            tier.add(minTime=start, maxTime=end, mark=speaker)
        tg.append(tier)

    speech_tier = IntervalTier(name="SM", minTime=0.0, maxTime=max_time)
    for segment in sm_segments:
        speech_tier.add(minTime=segment.start, maxTime=segment.end, mark=segment.label)
    tg.append(speech_tier)

    tg.write(args.output)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
