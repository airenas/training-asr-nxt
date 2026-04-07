import argparse
import json
import os
import sys
import tarfile
import time
import traceback
from dataclasses import dataclass
from io import BytesIO
from multiprocessing import JoinableQueue, Process, Queue
from pathlib import Path
from typing import List

import botocore
import numpy as np
import soundfile as sf
from tqdm import tqdm

from egs.analysis.local.calc_file_cuts import FileCuts
from preparation.logger import logger


@dataclass
class Chunk:
    source: str
    index: int
    items: List[FileCuts]


@dataclass
class TarChunk:
    source: str
    index: int
    items: List[Chunk]


def build_chunks(files, max_sec):
    current_items = []
    current_duration = 0.0
    current_source = None
    source_idx = 0

    for f in files:
        if current_source is None:
            current_source = f.source
            source_idx = 0

        # Source changed → reset counter
        if f.source != current_source:
            if current_items:
                yield Chunk(current_source, source_idx, current_items)
                source_idx += 1
            current_items = []
            current_duration = 0.0
            current_source = f.source
            source_idx = 0

        for seg in f.segments:
            dur = seg.end - seg.start

            if current_duration + dur > max_sec:
                if current_items:
                    yield Chunk(current_source, source_idx, current_items)
                    source_idx += 1
                current_items = [FileCuts(source=f.source, path=f.path, segments=[seg])]
                current_duration = dur
            else:
                if len(current_items) == 0 or current_items[-1].path != f.path:
                    current_items.append(FileCuts(source=f.source, path=f.path, segments=[seg]))
                else:
                    current_items[-1].segments.append(seg)
                current_duration += dur

    # last chunk
    if current_items:
        yield Chunk(current_source, source_idx, current_items)


class SimpleAudioWriter:
    def __init__(self, output_dir, sample_rate):
        self.output_dir = output_dir
        self.sample_rate = sample_rate
        logger.info(f"Initialized SimpleAudioWriter {output_dir} with sample rate {sample_rate}")

    def write(self, name, data):
        output_file = Path(self.output_dir) / name
        os.makedirs(output_file.parent, exist_ok=True)
        with open(output_file, "wb") as f:
            f.write(data.getbuffer())
        logger.debug(f"Saved {output_file}")


class S3AudioWriter:
    def __init__(self, bucket, sample_rate):
        import boto3
        self.bucket = bucket
        self.sample_rate = sample_rate
        self.s3_client = boto3.client(
            "s3",
            config=botocore.config.Config(
                retries={"max_attempts": 5, "mode": "standard"}
            ),
        )
        ## check access to bucket
        self.list_top_files(n=10)
        logger.info(f"Initialized S3AudioWriter with bucket {bucket}")

    def list_top_files(self, n=10, prefix=""):
        resp = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=n)
        files = []
        for obj in resp.get("Contents", []):
            files.append(obj["Key"])
        return files

    def write(self, name: str, data):
        start = time.perf_counter()
        size_mb = len(data.getbuffer()) / 1024 / 1024
        output_file = name
        logger.debug(f"Starting upload {output_file} {size_mb:.2f} MB")
        self.s3_client.upload_fileobj(data, self.bucket, str(output_file))
        elapsed = time.perf_counter() - start
        logger.debug(f"Uploaded {output_file}: {size_mb:.2f} MB in {elapsed:.2f}s ({size_mb / elapsed:.2f} MB/s)")


def calc_time(chunk: TarChunk):
    res = 0
    for chunk in chunk.items:
        for fc in chunk.items:
            for seg in fc.segments:
                res += seg.end - seg.start
    return res


def build_tar(param, n):
    items, source, index = [], None, 0
    for c in param:
        if len(items) >= n or c.source != source:
            if len(items) > 0:
                yield TarChunk(source=source, index=index, items=items)
            if c.source != source:
                source, index = c.source, 0
            else:
                index += 1
            items = []
        items.append(c)
    if len(items) > 0:
        yield TarChunk(source=source, index=index, items=items)


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Copy audio to destination from segments")
    parser.add_argument("--input", nargs='?', required=True, help="Jsonl with complete file segments")
    parser.add_argument("--audio-base", nargs='?', required=False, help="Base audio path to read from")
    parser.add_argument("--output_dir", nargs='?', required=False, help="Output dir to save cut audio")
    parser.add_argument("--bucket", nargs='?', required=False, help="Output bucket to save cut audio")
    parser.add_argument("--workers", nargs='?', required=False, default=8, type=int, help="Workers count")
    parser.add_argument("--sample-rate", nargs='?', required=False, default=16000, type=int, help="Sample rate")
    parser.add_argument("--audio-seconds", nargs='?', required=False, default=600, type=int,
                        help="Wanted audio files sizes")

    args = parser.parse_args(args=argv)

    logger.info(f"Input         : {args.input}")
    logger.info(f"Output dir    : {args.output_dir}")
    logger.info(f"Audio base    : {args.audio_base}")
    logger.info(f"Bucket        : {args.bucket}")
    logger.info(f"Workers       : {args.workers}")
    logger.info(f"Audio Secs    : {args.audio_seconds}")

    file_write = SimpleAudioWriter(args.output_dir, args.sample_rate)
    if args.bucket:
        logger.info(f"Using S3AudioWriter with bucket {args.bucket}")
        file_write = S3AudioWriter(args.bucket, args.sample_rate)
    else:
        logger.info(f"Using SimpleAudioWriter with output dir {args.output_dir}")

    files = []
    with open(args.input, "r", encoding="utf-8") as f:
        for i, line in enumerate(tqdm(f, desc="Load segments")):
            # if i > 10000:
            #     break
            data_dict = json.loads(line)
            segment = FileCuts.from_dict(data_dict)
            files.append(segment)
    logger.info(f"loaded {len(files)} files")

    logger.info(f"loaded {len(files)} sorting")
    files.sort(key=lambda s: (s.source, s.path), )
    tot_secs = 0
    for f in tqdm(files, desc="Calculate total audio seconds"):
        for s in f.segments:
            tot_secs += s.end - s.start
    logger.info(f"Total audio seconds: {tot_secs} ({tot_secs / 3600:.2f} hours)")

    logger.info(f"Start copying audio with {args.workers} workers")
    workers = []
    work_queue = JoinableQueue(maxsize=1)
    error_queue = Queue(maxsize=args.workers)
    for i in range(args.workers):
        p = Process(target=worker, args=(work_queue, args, file_write, error_queue))
        p.start()
        workers.append(p)

    skip = True
    with tqdm(total=tot_secs / 3600, desc="Copying", unit="h",
              bar_format="{l_bar}{bar}| "
                         "{n:.2f}/{total:.2f} {unit} "
                         "[{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for chunk in build_tar(build_chunks(files, args.audio_seconds), 30):
            if not error_queue.empty():
                logger.error("Error in worker")
                break
            fc = calc_time(chunk)
            # # crawl_034745.wav
            # for c in chunk:
            if chunk.source == "08kHz" and chunk.index == 120:
                skip = False
                logger.info(f"Continue from {chunk.source} {chunk.index}")
            if not skip:
                work_queue.put(chunk)
            pbar.update(fc / 3600)

    for _ in workers:
        work_queue.put(None)

    for p in workers:
        p.join()

    while not error_queue.empty():
        logger.error(f"Error in worker: {error_queue.get()}")

    logger.info(f"Done")


class AudioReader:
    def __init__(self):
        self.path = None
        self.data = None
        self.sr = None

    def read(self, path):
        if self.path != path:
            # logger.debug(f"open file {path}")
            self.data, self.sr = sf.read(path)
            self.path = path

        return self.data, self.sr


def worker(work_queue, args, file_writer, error_queue):
    logger.info("Worker started")

    base_audio_path = Path(args.audio_base)

    ar = AudioReader()

    while True:
        tar_chunk: TarChunk = work_queue.get()

        if tar_chunk is None:
            logger.info("Worker exiting")
            break
        tar_name = f"{tar_chunk.source}/{tar_chunk.index:06d}.tar"
        try:
            logger.debug(f"got task {tar_name} with {len(tar_chunk.items)} chunks")

            tar_buffer = BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode="w:") as tar:
                for chunk in tar_chunk.items:
                    output_file = f"{chunk.source}/{chunk.index:06d}.wav"
                    parts = []
                    for fc in chunk.items:
                        a_path = Path(fc.path)
                        if not a_path.is_absolute():
                            a_path = base_audio_path / a_path / "audio.16.wav"
                        # logger.info(f"loading {a_path}")
                        data, sr = ar.read(a_path)
                        if sr != args.sample_rate:
                            raise ValueError(f"Sample rate mismatch: {sr} != {args.sample_rate}")

                        for seg in fc.segments:
                            start = int(seg.start * sr)
                            end = int(seg.end * sr)
                            parts.append(data[start:end])

                    if not parts:
                        logger.warning(f"Empty chunk {output_file}")
                        continue

                    combined = np.concatenate(parts)
                    buffer = BytesIO()
                    sf.write(buffer, combined, sr, subtype="PCM_16", format="WAV")
                    buffer.seek(0)

                    info = tarfile.TarInfo(name=output_file)
                    info.size = len(combined)
                    tar.addfile(tarinfo=info, fileobj=buffer)

            tar_buffer.seek(0)
            file_writer.write(name=tar_name, data=tar_buffer)

            logger.debug(f"Saved {tar_name}")

        except Exception as e:
            tb = traceback.format_exc()
            error_queue.put(f"error in {tar_name}: {tb}")
            return

        finally:
            work_queue.task_done()


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
