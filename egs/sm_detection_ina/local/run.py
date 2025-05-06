import argparse
import os
import sys
from collections import Counter
from functools import partial

import dask.bag as db
import ffmpeg
from dask.distributed import Client
from distributed import LocalCluster
from inaSpeechSegmenter import Segmenter

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


def ina_segments(file_path, input_dir, output_dir: str, segmenter: Segmenter):
    logger.info(f"Processing file for ina segments: {file_path}")

    def segment_func(file_path_: str):
        logger.info(f"call segments: {file_path}...")
        segments = segmenter(file_path_)
        logger.info(f"got segments: {file_path}, {segments}")
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


def calc_segments_sum(data):
    res = Counter()
    for (type_, from_, to_) in data:
        res[type_] += to_ - from_
    return res


def calculate_total_smn(file_bag):
    def combine_smn(accum, item):
        logger.info(f"combiner smn acc={accum}, item={item}")
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

    # with set(scheduler='synchronous'):  # Remove this block in prod
    return file_bag.fold(
        binop=combine_smn,
        combine=combine_smn,
        initial=(0, 0, 0)
    ).compute()


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

    seg = Segmenter(vad_engine="smn", detect_gender=False, ffmpeg="ffmpeg", batch_size=1)

    all_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(args.input)
        for f in files
    ]
    logger.info(f"Found {len(all_files)} files to process")

    file_bag = db.from_sequence(all_files, npartitions=args.workers * 4)

    file_size_args = partial(file_size, input_dir=args.input, output_dir=args.output)
    audio_length_args = partial(audio_length, input_dir=args.input, output_dir=args.output)
    ina_segments_args = partial(ina_segments, input_dir=args.input, output_dir=args.output, segmenter=seg)

    processed_sizes = file_bag.map(file_size_args)
    processed_lengths = file_bag.map(audio_length_args)
    processed_ina = file_bag.map(ina_segments_args)

    total_size = calculate_total_sum("size", processed_sizes)
    logger.info(f"Total size: {total_size / 1024 / 1024} MB")
    total_audio_length = calculate_total_sum("audio len", processed_lengths)
    logger.info(f"Total audio length: {total_audio_length / 60:.2f} minutes")

    total_music, total_non_music, sil = calculate_total_smn(processed_ina)
    logger.info(f"Total music length: {total_music / 60:.2f} minutes")
    logger.info(f"Total non-music length: {total_non_music / 60:.2f} minutes")
    logger.info(f"Total silence length: {sil / 60:.2f} minutes")

    client.shutdown()


if __name__ == "__main__":
    main(sys.argv[1:])
