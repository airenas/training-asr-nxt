import argparse
import os
import random
import sys
from pathlib import Path

import soundfile as sf
from tqdm import tqdm

from preparation.logger import logger


def save_list(out, root, files):
    with open(out, "w") as f:
        absolute_root = Path(root).absolute()
        f.write(str(absolute_root) + "\n")
        for (p, n) in files:
            p = Path(p).absolute()
            # remove root from path
            p = p.relative_to(absolute_root)
            f.write(f"{str(p)}\t{n}\n")


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Make wav list")
    parser.add_argument("--input", nargs='?', required=True, help="Dir of wavs")
    parser.add_argument("--output", nargs='?', required=True, help="Dir to wanted output files")

    args = parser.parse_args(args=argv)

    train = Path(args.output) / "train.tsv"
    valid = Path(args.output) / "valid.tsv"
    test = Path(args.output) / "test.tsv"

    logger.info(f"Dir         : {args.input}")
    logger.info(f"Output Test : {test}")
    logger.info(f"Output Train : {train}")
    logger.info(f"Output Valid  : {valid}")

    all_files = []
    for root, dirs, files in tqdm(os.walk(args.input), desc="Processing wavs"):
        for file in files:
            if Path(file).suffix.lower() not in {".wav", ".flac"}:
                continue
            full_path = Path(str(os.path.join(root, file)))
            all_files.append(full_path)
    logger.info(f"Found {len(all_files)} wav/flac files")
    all_data = []
    for file in tqdm(all_files, desc="Processing wavs"):
        data, sr = sf.read(file)
        nsamples = len(data)
        all_data.append((file, nsamples))

    logger.info(f"Found {len(all_files)} wav/flac files")
    random.seed(42)
    random.shuffle(all_data)

    train_ratio = 0.95
    valid_ratio = 0.025

    n = len(all_data)
    n_train = int(n * train_ratio)
    n_valid = int(n * valid_ratio)

    train_files = all_data[:n_train]
    valid_files = all_data[n_train:n_train + n_valid]
    test_files = all_data[n_train + n_valid:]

    save_list(train, args.input, train_files)
    save_list(valid, args.input, valid_files)
    save_list(test, args.input, test_files)

    logger.info(f"Done")


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
