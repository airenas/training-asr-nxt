import argparse
import os
import sys

from tqdm import tqdm

from preparation.logger import logger


def merge_km(split, n_shards, base_dir):
    output_file = os.path.join(base_dir, f"{split}.km")

    with open(output_file, "w") as out_f:
        for rank in tqdm(range(n_shards)):
            input_file = os.path.join(base_dir, f"{split}_{rank}_{n_shards}.km")

            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Missing shard file: {input_file}")

            with open(input_file, "r") as in_f:
                for line in in_f:
                    out_f.write(line)


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make wav list")
    parser.add_argument("--split", required=True, type=str)
    parser.add_argument("--n-shards", type=int, required=True)
    parser.add_argument("--base-dir", required=True)

    args = parser.parse_args(args=argv)

    logger.info(f"Base dir    : {args.base_dir}")
    logger.info(f"N shards    : {args.n_shards}")
    logger.info(f"split       : {args.split}")

    merge_km(args.split, args.n_shards, args.base_dir)

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
