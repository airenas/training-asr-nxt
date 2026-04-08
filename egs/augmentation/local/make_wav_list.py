import argparse
import os
import sys
from pathlib import Path

from tqdm import tqdm

from preparation.logger import logger


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make audio.rttm from vad segments")
    parser.add_argument("--dir", nargs='?', required=True, help="Dir of wavs")
    parser.add_argument("--out", nargs='?', required=True, help="Path to wanted output file")

    args = parser.parse_args(args=argv)

    logger.info(f"Dir         : {args.dir}")
    logger.info(f"Output      : {args.out}")

    with open(args.out, "w") as f:
        for root, dirs, files in tqdm(os.walk(args.dir), desc="Processing wavs"):
            for file in files:
                if not file.endswith(".wav"):
                    continue
                full_path = Path(str(os.path.join(root, file))).absolute()
                name = os.path.splitext(file)[0]
                f.write(f"{name} {str(full_path)}\n")

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
