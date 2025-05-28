import argparse
import json
import os
import sys

from preparation.logger import logger


def collect_embeddings(input_dir, file_name):
    res = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file == file_name:
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    for line in f:
                        d = json.loads(line)
                        d["f"] = file_path
                        res.append(d)
    return res



def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Collect all embeddings into one file")
    parser.add_argument("--input", nargs='?', required=True, help="Input dir")
    parser.add_argument("--output", nargs='?', required=True, help="File")
    args = parser.parse_args(args=argv)

    logger.info(f"Input dir    : {args.input}")
    logger.info(f"Output file  : {args.output}")

    res = collect_embeddings(args.input, "speaker.embeddings.jsonl")

    with open(args.output, "w") as f:
        for v in res:
            f.write(json.dumps(v) + "\n")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
