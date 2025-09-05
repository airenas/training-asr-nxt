import argparse
import os
import sys

from preparation.logger import logger
from preparation.utils.audio import Audio


def diarization(audio_path):
    from pyannote.audio import Pipeline
    import torch

    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token=os.getenv('HF_API_TOKEN'))

    audio = Audio.from_file(audio_path)

    pipeline.to(torch.device("cuda"))
    res = pipeline({"waveform": audio.waveform, "sample_rate": audio.sample_rate})
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio diarization")
    parser.add_argument("--input_wav", nargs='?', required=True, help="Audio file")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input wav file   : {args.input_wav}")
    logger.info(f"Output file  : {args.output}")
    res = diarization(args.input_wav)
    logger.info(f"Got results: {res}")

    with open(args.output, "w") as f:
        if not isinstance(res, list):
            res.write_rttm(f)

        else:
            logger.warn("Got empty result")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
