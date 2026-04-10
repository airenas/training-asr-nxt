import argparse
import io
import os
import random
import shutil
import subprocess
import sys
import tarfile
import time
import traceback
from dataclasses import dataclass
from multiprocessing import JoinableQueue, Process, Queue
from pathlib import Path
from typing import List, Dict

import botocore
import soundfile as sf
from tqdm import tqdm

from preparation.logger import logger


@dataclass
class TarChunk:
    name: str
    dir: str
    orig: List[str]
    augmented: List[str]


@dataclass
class Msg:
    type: str
    tar: str
    value: str


@dataclass
class AugmentMsg:
    tar: str
    file: str
    out: str
    rir: str
    rir_delay: float
    noise: str
    noise_a: int
    comp: str
    comp_a: int


class S3AudioLoader:
    def __init__(self, bucket, bucker_save):
        import boto3
        self.bucket = bucket
        self.bucket_save = bucker_save
        self.s3_client = boto3.client(
            "s3",
            config=botocore.config.Config(
                retries={"max_attempts": 5, "mode": "standard"}
            ),
        )
        ## check access to bucket
        self.list_top_files(n=10)
        logger.info(f"Initialized S3AudioLoader with bucket {bucket}")

    def list_top_files(self, n=10):
        bucket, prefix = self.bucket.split("/", 1)
        resp = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=n)
        files = []
        for obj in resp.get("Contents", []):
            files.append(obj["Key"])
        return files

    def load(self, name):
        start = time.perf_counter()
        bucket, key = self.bucket.split("/", 1)
        name = f"{key}/{name}"
        logger.debug(f"Loading {name}")
        res = io.BytesIO()
        self.s3_client.download_fileobj(bucket, name, res)
        elapsed = time.perf_counter() - start
        logger.debug(f"Loaded {name} from S3 in {elapsed:.2f} seconds")
        res.seek(0)
        return res

    def write(self, name: str, data):
        start = time.perf_counter()
        bucket, key = self.bucket_save.split("/", 1)
        size_mb = len(data.getbuffer()) / 1024 / 1024
        output_file = f"{key}/{name}"
        logger.debug(f"Starting upload {output_file} {size_mb:.2f} MB")
        self.s3_client.upload_fileobj(data, bucket, str(output_file))
        elapsed = time.perf_counter() - start
        logger.info(f"Uploaded {output_file}: {size_mb:.2f} MB in {elapsed:.2f}s ({size_mb / elapsed:.2f} MB/s)")


def load_rirs(dir):
    file = os.path.join(dir, "rir_valid_list")
    with open(file, "r") as f:
        return [line.strip() for line in f if line.strip()]


def load_rir_delays(dir):
    file = os.path.join(dir, "RIR", "rir_delays")
    with open(file, "r") as f:
        res = {}
        for line in f:
            if line.strip():
                parts = line.strip().split()
                res[parts[0]] = float(parts[1])
        return res


def load_noises(dir):
    file = os.path.join(dir, "noises.txt")
    with open(file, "r") as f:
        return [line.strip() for line in f if line.strip()]


def load_compressions(dir):
    file = os.path.join(dir, "compressions.txt")
    with open(file, "r") as f:
        res = []
        for line in f:
            if line.strip():
                parts = line.strip().split()
                res.append((parts[0], int(parts[1])))
    for _ in range(len(res)):
        res.append(("none", 0))
    return res


class AugmentInfo:
    def __init__(self, dir):
        self.rirs = load_rirs(dir)
        self.rir_delays = load_rir_delays(dir)
        for rir in self.rirs:
            if rir not in self.rir_delays:
                raise ValueError(f"RIR {rir} does not have a delay in rir_delays file")
        self.noises = load_noises(dir)
        self.compressions = load_compressions(dir)

    def generate(self, file, i, tar) -> AugmentMsg:
        rir = random.choice(self.rirs)
        noise = random.choice(self.noises)
        noise_a = random.randint(3, 25)
        comp = random.choice(self.compressions)
        dir = Path(file).parent
        f_name = Path(file).stem
        return AugmentMsg(tar=tar, file=file, out=f"{dir}/{f_name}_{i + 1:02d}.wav", rir=rir,
                          rir_delay=self.rir_delays[rir],
                          noise=noise, noise_a=noise_a, comp=comp[0], comp_a=comp[1])


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Copy audio to destination from segments")
    parser.add_argument("--s3-bucket", nargs='?', required=True, help="S3 bucket to read segments from")
    parser.add_argument("--s3-bucket-output", nargs='?', required=True, help="S3 bucket to write results")
    parser.add_argument("--s3-from", nargs='?', required=False, default=0, type=int, help="Index to start from")
    parser.add_argument("--s3-to", nargs='?', required=False, default=999999, type=int, help="Index to end at")
    parser.add_argument("--workers", nargs='?', required=False, default=4, type=int, help="Augment workers count")
    parser.add_argument("--aug-dir", nargs='?', required=False, help="Dir of augmentation code folder")
    parser.add_argument("--tmp-dir", nargs='?', required=False, help="Temporary dir of downloaded files")

    args = parser.parse_args(args=argv)

    logger.info(f"S3          : {args.s3_bucket} to {args.s3_bucket_output} from {args.s3_from} to {args.s3_to}")
    logger.info(f"Aug workers : {args.workers}")
    logger.info(f"Aug dir     : {args.aug_dir}")
    logger.info(f"Tmp dir     : {args.tmp_dir}")

    aug_gen = AugmentInfo(args.aug_dir)
    logger.info(f"Rir          : {len(aug_gen.rirs)}")
    logger.info(f"Rir delays   : {len(aug_gen.rir_delays)}")
    logger.info(f"Noises       : {aug_gen.noises}")
    logger.info(f"Compressions : {aug_gen.compressions}")

    s3_dwn = S3AudioLoader(args.s3_bucket, args.s3_bucket_output)
    s3_upl = S3AudioLoader(args.s3_bucket, args.s3_bucket_output)
    workers = []
    load_queue = JoinableQueue(maxsize=10)
    manager_queue = JoinableQueue(maxsize=100)
    augment_queue = JoinableQueue(maxsize=100)
    error_queue = Queue(maxsize=args.workers + 3)
    save_queue = JoinableQueue(maxsize=10)

    p_loader = Process(target=loader,
                       args=(load_queue, augment_queue, error_queue, manager_queue, s3_dwn, aug_gen.generate, args.tmp_dir))
    p_loader.start()

    p_manager = Process(target=manager, args=(manager_queue, save_queue, error_queue))
    p_manager.start()

    w_saver = []
    for i in range(4):
        p_saver = Process(target=saver, args=(save_queue, error_queue, s3_upl))
        p_saver.start()
        w_saver.append(p_saver)

    for i in range(args.workers):
        p = Process(target=augmenter, args=(augment_queue, manager_queue, error_queue, args.aug_dir + "/run_one.sh", i))
        p.start()
        workers.append(p)

    for i in tqdm(range(args.s3_from, args.s3_to)):
        load_queue.put(i)

    load_queue.put(None)
    logger.info("waiting for load to finish")
    p_loader.join()
    for _ in workers:
        augment_queue.put(None)
    logger.info("waiting for aug to finish")
    for p in workers:
        p.join()
    manager_queue.put(None)
    logger.info("waiting for manager to finish")
    p_manager.join()
    logger.info("waiting for saver to finish")

    for _ in w_saver:
        save_queue.put(None)
    for p_saver in w_saver:
        p_saver.join()

    logger.info("waiting to finish")

    is_err = False
    while not error_queue.empty():
        logger.error(f"Error in worker: {error_queue.get()}")
        is_err = True

    if is_err:
        raise RuntimeError("Errors occurred in workers, check logs for details")

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


def loader(load_queue, augment_queue, error_queue, manager_queue, file_loader: S3AudioLoader, augment_info, tmp_dir):
    logger.info("Worker started")

    is_error = False
    while True:
        (_id) = load_queue.get()
        if _id is None:
            logger.info("Worker exiting")
            break

        tar_name = f"{_id:06d}.tar"
        if not error_queue.empty():
            is_error = True

        if is_error:
            logger.warning(f"Worker skipping due to previous error {tar_name}")
            load_queue.task_done()
            continue
        logger.debug(f"Got load id {_id}")
        try:
            logger.debug(f"got task {tar_name}")
            out_dir = f"{tmp_dir}/{_id:06d}"
            extract_dir = f"{out_dir}"
            os.makedirs(extract_dir, exist_ok=True)

            tar_buffer = file_loader.load(tar_name)
            logger.debug(f"Loaded {tar_name} from S3, extracting")
            files = []
            with tarfile.open(fileobj=tar_buffer, mode="r:*") as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.endswith(".wav"):
                        # Know the name
                        wav_name = member.name
                        logger.debug(f"Processing: {wav_name}")
                        local_path = os.path.join(extract_dir, member.name)
                        os.makedirs(Path(local_path).parent, exist_ok=True)
                        with tar.extractfile(member) as src, open(local_path, "wb") as out:
                            shutil.copyfileobj(src, out)
                        files.append(local_path)
                        logger.debug(f"Extracted to: {local_path}")
            logger.debug(f"Saved {tar_name}")
            manager_queue.put(Msg(type="tar", tar=tar_name, value=out_dir))
            for f in files:
                manager_queue.put(Msg(type="tar_file", tar=tar_name, value=f))
            for f in files:
                for i in range(3):
                    pass
                    augment_queue.put((augment_info(f, i, tar_name)))


        except Exception as e:
            logger.error(f"Failed to process {tar_name}: {e}")
            tb = traceback.format_exc()
            error_queue.put(f"error in {tar_name}: {tb}")
            is_error = True

        finally:
            load_queue.task_done()


def saver(save_queue, error_queue, s3: S3AudioLoader):
    logger.info("Worker started")

    is_error = False
    while True:
        if not error_queue.empty():
            is_error = True

        msg: TarChunk = save_queue.get()
        try:
            if msg is None:
                logger.info("Worker exiting")
                break

            if is_error:
                logger.warning(f"Worker skipping due to previous error")
                continue

            logger.debug(f"got save task {msg.name}, dir {msg.dir} files: {len(msg.augmented)}")

            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode="w:") as tar:
                # for f in tqdm(msg.augmented, desc=f"Saving {msg.name} to tar"):
                for f in msg.augmented:
                    f = Path(f)
                    # read wav
                    data, sr = sf.read(f)

                    # write to flac in memory
                    buf = io.BytesIO()
                    sf.write(buf, data, sr, format="FLAC")
                    buf.seek(0)

                    # change name to .flac
                    arcname = os.path.relpath(f, msg.dir)
                    arcname = str(Path(arcname).with_suffix(".flac"))

                    # add to tar
                    info = tarfile.TarInfo(name=arcname)
                    info.size = buf.getbuffer().nbytes
                    info.mtime = int(time.time())

                    tar.addfile(info, fileobj=buf)

            tar_buffer.seek(0)
            s3.write(msg.name, tar_buffer)
            logger.debug(f"Saved {msg.name} to S3")

            logger.debug(f"Cleaning up {msg.dir}")
            shutil.rmtree(msg.dir)

        except Exception as e:
            logger.error(f"Failed to save {msg.name}: {e}")
            tb = traceback.format_exc()
            error_queue.put(f"error in save {msg.name}: {tb}")
            is_error = True

        finally:
            save_queue.task_done()


def manager(manager_queue, save_queue, error_queue):
    logger.info("Worker started")

    state: Dict[str, TarChunk] = {}

    is_error = False
    while True:
        if not error_queue.empty():
            is_error = True
        (msg) = manager_queue.get()
        try:
            if msg is None:
                logger.info("Worker manager exiting")
                break

            if is_error:
                logger.warning(f"Worker manager skipping due to previous error")
                continue

            if msg.type == "tar":
                state[msg.tar] = TarChunk(name=msg.tar, orig=[], augmented=[], dir=msg.value)
            if msg.type == "tar_file":
                state[msg.tar].orig.append(msg.value)
            if msg.type == "file_done":
                st = state[msg.tar]
                st.augmented.append(msg.value)
                if len(st.orig) * 3 == len(st.augmented):
                    logger.debug(f"All files ({len(st.orig)}:{len(st.augmented)}) done for {msg.tar}, saving tar")
                    save_queue.put(st)
                    del state[msg.tar]
        except Exception as e:
            tb = traceback.format_exc()
            error_queue.put(f"error: {tb}")
            is_error = True

        finally:
            manager_queue.task_done()


def augmenter(augment_queue, manager_queue, error_queue, cmd, wrk_id):
    logger.info("Worker started")

    p_dir = Path(cmd).parent
    cmd = f"./{Path(cmd).name}"

    is_error = False
    while True:
        if not error_queue.empty():
            is_error = True
        (msg) = augment_queue.get()
        try:
            if msg is None:
                logger.info("Worker exiting")
                break

            if is_error:
                logger.warning(f"Worker skipping due to previous error")
                continue
            cmd_array = [cmd, Path(msg.file).absolute(), msg.rir, str(msg.rir_delay), msg.noise, str(msg.noise_a),
                         msg.comp, str(msg.comp_a), Path(msg.out).absolute(), str(wrk_id)]
            logger.debug(f"Run {cmd_array}")
            subprocess.run(
                cmd_array,
                cwd=p_dir,
                check=True
            )

            logger.debug(f"Finished augmenting file {msg.file}, saving as {msg.out}")
            manager_queue.put(Msg(type="file_done", tar=msg.tar, value=msg.out))

        except Exception as e:
            tb = traceback.format_exc()
            error_queue.put(f"error: {tb}")
            is_error = True

        finally:
            augment_queue.task_done()


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
