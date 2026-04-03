import argparse
import json
import os
import sys
from dataclasses import dataclass
from multiprocessing import JoinableQueue, Process
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

    def write(self, name, data):
        output_file = Path(self.output_dir) / name
        os.makedirs(output_file.parent, exist_ok=True)
        sf.write(output_file, data, self.sample_rate, subtype="PCM_16")
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

    def write(self, name: Path, data):
        from io import BytesIO
        output_file = name
        buffer = BytesIO()
        sf.write(buffer, data, self.sample_rate, subtype="PCM_16", format="WAV")
        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, self.bucket, str(output_file))
        logger.debug(f"Saved {output_file} to bucket {self.bucket}")


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
        for i, line in enumerate(tqdm(f)):
            # if i > 10000:
            #     break
            data_dict = json.loads(line)
            segment = FileCuts.from_dict(data_dict)
            files.append(segment)
    logger.info(f"loaded {len(files)} files")

    logger.info(f"loaded {len(files)} sorting")
    files.sort(key=lambda s: (s.source, s.path), )
    tot_secs = 0
    for f in tqdm(files):
        for s in f.segments:
            tot_secs += s.end - s.start
    logger.info(f"Total audio seconds: {tot_secs} ({tot_secs / 3600:.2f} hours)")

    logger.info(f"Start copying audio with {args.workers} workers")
    workers = []
    work_queue = JoinableQueue(maxsize=100)
    for i in range(args.workers):
        p = Process(target=worker, args=(work_queue, args, file_write))
        p.start()
        workers.append(p)

    with tqdm(total=len(files), desc="Copying") as pbar:
        for chunk in build_chunks(files, args.audio_seconds):
            fc = len(chunk.items)
            work_queue.put(chunk)
            pbar.update(fc)

    for _ in workers:
        work_queue.put(None)

    for p in workers:
        p.join()

    logger.info(f"Done")


def worker(work_queue, args, file_writer):
    logger.info("Worker started")

    base_audio_path = Path(args.audio_base)

    while True:
        chunk = work_queue.get()

        if chunk is None:
            logger.info("Worker exiting")
            break

        try:
            bucket_id = chunk.index // 1000
            output_file = Path(chunk.source) / f"{bucket_id:03d}" / f"{chunk.source}_{chunk.index:06d}.wav"
            parts = []
            for fc in chunk.items:
                a_path = Path(fc.path)
                if not a_path.is_absolute():
                    a_path = base_audio_path / a_path / "audio.16.wav"
                # logger.info(f"loading {a_path}")
                data, sr = sf.read(a_path)
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

            file_writer.write(output_file, combined)

            logger.debug(f"Saved {output_file}")

        except Exception as e:
            logger.exception(f"Error processing chunk {chunk.source}_{chunk.index}: {e}")

        finally:
            work_queue.task_done()


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
