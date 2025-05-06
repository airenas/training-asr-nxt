import argparse
import os
import sys
from functools import partial

import dask.bag as db
import ffmpeg
from dask.distributed import Client
from distributed import LocalCluster

from preparation.cache.file import Params, file_cache_func
from preparation.logger import logger


def file_size(file_path, input_dir, output_dir: str):
    def file_size_func(file_path_: str):
        return os.path.getsize(file_path_)

    return file_cache_func(file_path,
                           Params(input_dir=input_dir, output_dir=output_dir, name="file.size",
                                  run_f=file_size_func, deserialize_func=Params.to_int))


def audio_length(file_path, input_dir, output_dir: str):
    def len_func(file_path_: str):
        try:
            probe = ffmpeg.probe(file_path_)
            duration = float(probe['format']['duration'])
            return duration
        except Exception as e:
            raise e

    return file_cache_func(file_path,
                           Params(input_dir=input_dir, output_dir=output_dir, name="audio.len", run_f=len_func,
                                  deserialize_func=Params.to_float))


def calculate_total_sum(name, file_bag):
    def combiner(acc, item):
        logger.debug(f"combiner name={name} acc={acc}, item={item}")
        if item.get("ok"):
            return acc + item["result"]
        else:
            logger.error(f"Error calculate_total_size {item['file_path']}: {item['error']}")
            return acc

    def merge(acc1, acc2):
        logger.debug(f"merge acc1={acc1}, acc2={acc2}")
        return acc1 + acc2

    return file_bag.fold(combiner, merge, initial=0).compute()


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio processing")
    parser.add_argument("--input", nargs='?', required=True, help="Input dir")
    parser.add_argument("--output", nargs='?', required=True, help="Output dir")
    parser.add_argument("--workers", nargs='?', required=True, default=4, type=int, help="Workers count")
    args = parser.parse_args(args=argv)

    logger.info(f"Input dir: {args.input}")
    logger.info(f"Output dir: {args.output}")
    logger.info(f"Workers   : {args.workers}")

    cluster = LocalCluster(n_workers=args.workers, threads_per_worker=1)
    client = Client(cluster)
    logger.info(f"status: {client.dashboard_link}")

    all_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(args.input)
        for f in files
    ]
    logger.info(f"Found {len(all_files)} files to process")

    file_bag = db.from_sequence(all_files, npartitions=args.workers * 10)

    file_size_args = partial(file_size, input_dir=args.input, output_dir=args.output)
    audio_length_args = partial(audio_length, input_dir=args.input, output_dir=args.output)

    processed_sizes = file_bag.map(file_size_args)
    processed_lengths = file_bag.map(audio_length_args)

    total_size = calculate_total_sum("size", processed_sizes)
    logger.info(f"Total size: {total_size / 1024 / 1024 :.2f} MB")
    logger.info(f"Total size: {total_size / 1024 / 1024 / 1024 :.2f} GB")
    total_audio_length = calculate_total_sum("audio len", processed_lengths)
    logger.info(f"Total audio length: {total_audio_length / 60 :.2f} in minutes")
    logger.info(f"Total audio length: {total_audio_length / (60 * 60) :.2f} in hours")

    client.shutdown()


if __name__ == "__main__":
    main(sys.argv[1:])
