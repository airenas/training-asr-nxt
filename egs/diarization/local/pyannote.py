import argparse
import io
import os
import sys

import torchaudio

from preparation.logger import logger
from preparation.utils.sm_segments import load_segments


def extracted_audio(waveform, sample_rate, segment):
    start_sample = int(segment.start * sample_rate)
    end_sample = int(segment.end * sample_rate)
    part = waveform[:, start_sample:end_sample]

    buffer = io.BytesIO()
    torchaudio.save(buffer, part, sample_rate, format="wav")
    buffer.seek(0)
    return buffer


def cut_audio(waveform, sample_rate, segments):
    import torch

    combined_waveform = []
    for segment in segments:
        start_sample = int(segment.start * sample_rate)
        end_sample = int(segment.end * sample_rate)
        part = waveform[:, start_sample:end_sample]
        combined_waveform.append(part)

    res = torch.cat(combined_waveform, dim=1)
    return res


def diarization(audio_path, segments):
    from pyannote.audio import Pipeline
    import torch

    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token=os.getenv('HF_API_TOKEN'))

    waveform, sample_rate = torchaudio.load(audio_path)
    speech_wave = cut_audio(waveform, sample_rate, segments)

    pipeline.to(torch.device("cuda"))
    res = pipeline({"waveform": speech_wave, "sample_rate": sample_rate})
    return res


def select_segments(segments):
    return [seg for seg in segments if seg.label == "speech"]


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio diarizatiopn")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")
    segments = load_segments(args.input)
    speech_segments = select_segments(segments)
    res = []
    if len(speech_segments) > 0:
        res = diarization(args.input, speech_segments)
        logger.info(f"Got results: {res}")

    with open(args.output, "w") as f:
        if not isinstance(res, list):
            res.write_rttm(f)
        else:
            logger.warn("Got empty result")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
