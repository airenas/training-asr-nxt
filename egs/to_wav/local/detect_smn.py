import os
print("PYTHONPATH in Python:", os.environ.get("PYTHONPATH"))
import argparse
import sys

from inaSpeechSegmenter import Segmenter

from preparation.logger import logger
from preparation.cache.file import Params


def init_segmenter():
    return Segmenter(vad_engine="smn", detect_gender=False, ffmpeg="ffmpeg", batch_size=1)


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Detects speech music, silence")
    parser.add_argument("--input", nargs='?', required=True, help="Input file")
    parser.add_argument("--output", nargs='?', required=True, help="Output file")
    args = parser.parse_args(args=argv)

    logger.info(f"Input    : {args.input}")
    logger.info(f"Output   : {args.output}")

    segmenter = init_segmenter()
    segments = segmenter(args.input)
    logger.info(f"Got segments: {segments}")
    with open(args.output, "w") as f:
        for segment in segments:
            f.write(Params.to_json_str(segment))

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
