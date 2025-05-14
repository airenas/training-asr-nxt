import argparse
import math
import os
import sys
from collections import Counter
from functools import partial

import dask.bag as db
from dask.distributed import Client
from distributed import LocalCluster, get_worker
from inaSpeechSegmenter import Segmenter

from preparation.cache.file import Params, file_cache_func
from preparation.logger import logger


def ina_segments(file_path, input_dir, output_dir: str):
    logger.debug(f"Processing file for ina segments: {file_path}")

    worker = get_worker()
    if not hasattr(worker, 'segmenter'):
        worker.segmenter = init_segmenter()

    def segment_func(file_path_: str):
        logger.debug(f"call segments: {file_path}...")
        segments = worker.segmenter(file_path_)
        logger.debug(f"got segments: {file_path}, {segments}")
        return segments

    return file_cache_func(
        file_path,
        Params(
            input_dir=input_dir,
            output_dir=output_dir,
            name="audio.ina_segments",
            run_f=segment_func,
            deserialize_func=Params.to_json,
            serialize_func=Params.to_json_str,
        ),
    )


def calc_segments_sum(data):
    res = Counter()
    for (type_, from_, to_) in data:
        res[type_] += to_ - from_
    return res


def calculate_total_smn(file_bag):
    def combine_smn(accum, item):
        logger.debug(f"combiner smn acc={accum}, item={item}")
        if item.get('ok'):
            data = item['result']
            count = calc_segments_sum(data)
            logger.info(f"count: {count}")
            return (
                accum[0] + count.get("music", 0),
                accum[1] + count.get("speech", 0),
                accum[2] + count.get("noEnergy", 0),
            )
        else:
            logger.error(f"Error processing {item.get('file_path')}: {item.get('error')}")
            return accum  # Just return current totals unchanged

    def merge(tot1, tot2):
        logger.info(f"merge tot1={tot1}, tot2={tot2}")
        return tuple(a + b for a, b in zip(tot1, tot2))

    return file_bag.fold(
        binop=combine_smn,
        combine=merge,
        initial=(0, 0, 0)
    ).compute()


def init_segmenter():
    return Segmenter(vad_engine="smn", detect_gender=False, ffmpeg="ffmpeg", batch_size=1)

def to_mb(size_bytes):
    """Convert bytes to megabytes."""
    return size_bytes / (1024 * 1024)

def log_file_size_histogram(sizes, bin_count=100):
    min_size = min(sizes)
    max_size = max(sizes)
    bin_size = (max_size - min_size) / bin_count

    bins = Counter([math.floor((size - min_size) / bin_size) for size in sizes])

    all = 0
    for bin_index, count in sorted(bins.items()):
        all += count
        logger.info(f"Bin {to_mb(bin_index * bin_size):.2f}-{to_mb((bin_index + 1) * bin_size):.2f} mb: {count}:{all} files")


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio processing")
    parser.add_argument("--input", nargs='?', required=True, help="Input dir")
    parser.add_argument("--output", nargs='?', required=True, help="Output dir")
    parser.add_argument("--workers", nargs='?', required=True, default=4, type=int, help="Workers count")
    parser.add_argument("--memory_limit", nargs='?', required=True, default="0", type=str, help="Worker's max memory limit")
    parser.add_argument("--limit_mb", nargs='?', default=50, type=float,
                        help="File size limit")
    args = parser.parse_args(args=argv)

    logger.info(f"Input dir    : {args.input}")
    logger.info(f"Output dir   : {args.output}")
    logger.info(f"Workers      : {args.workers}")
    logger.info(f"Worker's mem : {args.memory_limit}")
    logger.info(f"File limit   : {args.limit_mb}mb")

    cluster = LocalCluster(n_workers=args.workers, threads_per_worker=1, memory_limit=args.memory_limit)
    client = Client(cluster)
    logger.info(f"status: {client.dashboard_link}")

    # seg = Segmenter(vad_engine="smn", detect_gender=False, ffmpeg="ffmpeg", batch_size=1)

    all_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(args.input)
        for f in files
    ]
    logger.info(f"Found {len(all_files)} files to process")
    filtered_files, skipped = [], 0
    sizes = []
    logger.info(f"Calculating sizes ...")
    for f in all_files:
        if f.endswith(".m4a"):
            size = os.path.getsize(f)
            sizes.append(size)
            if size < (args.limit_mb * 1024 * 1024):
                filtered_files.append(f)
            else:
                skipped += 1

    log_file_size_histogram(sizes, 20)

    logger.info(f"Filtered {len(filtered_files)} files, skipped {skipped} files")

    file_bag = db.from_sequence(filtered_files, npartitions=args.workers * 50)

    ina_segments_args = partial(ina_segments, input_dir=args.input, output_dir=args.output)

    processed_ina = file_bag.map(ina_segments_args)

    total_music, total_non_music, sil = calculate_total_smn(processed_ina)
    logger.info(f"Total music length: {total_music / 3600:.2f} h")
    logger.info(f"Total non-music length: {total_non_music / 3600:.2f} h")
    logger.info(f"Total silence length: {sil / 3600:.2f} h")

    client.shutdown()


if __name__ == "__main__":
    main(sys.argv[1:])
