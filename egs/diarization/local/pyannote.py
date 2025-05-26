import argparse
import bisect
import io
import os
import sys
from typing import List

import torchaudio
from pyannote.core import Segment as PyannoteSegment, Annotation

from preparation.logger import logger
from preparation.utils.audio import Audio
from preparation.utils.sm_segments import load_segments, select_speech, Segment


def extracted_audio(waveform, sample_rate, segment):
    start_sample = int(segment.start * sample_rate)
    end_sample = int(segment.end * sample_rate)
    part = waveform[:, start_sample:end_sample]

    buffer = io.BytesIO()
    torchaudio.save(buffer, part, sample_rate, format="wav")
    buffer.seek(0)
    return buffer


def diarization(audio_path, segments):
    from pyannote.audio import Pipeline
    import torch

    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token=os.getenv('HF_API_TOKEN'))

    audio = Audio.from_file(audio_path)
    audio_speech = audio.cut(segments)

    pipeline.to(torch.device("cuda"))
    res = pipeline({"waveform": audio_speech.waveform, "sample_rate": audio_speech.sample_rate})
    return res


def fix_time(shifts, time):
    """
    Find first greater value and return
    """
    pos = bisect.bisect_right(shifts, time, key=lambda x: x[0])
    if pos == 0:
        return shifts[pos][1] + time
    return shifts[pos - 1][1] + time


def cut_non_speech_regions(speakers_seg: List[Segment], speech_seg: List[Segment]) -> List[Segment]:
    result = []
    for seg in speakers_seg:
        for speech in speech_seg:
            if seg.end <= speech.start:
                break
            start = max(seg.start, speech.start)
            end = min(seg.end, speech.end)
            if start < end:
                result.append(Segment(label=seg.label, start=start, end=end))
    return result


def restore_time(data: Annotation, speech_segments: List[Segment]) -> Annotation:
    res = Annotation()
    offset = 0.0
    segments = list(data.itertracks(yield_label=True))
    segments.sort(key=lambda x: x[0].start)  # Sort segments by start time
    shifts = []
    for segment in speech_segments:
        shifts.append((offset, segment.start - offset))
        offset += segment.end - segment.start

    fixed_segments = []
    for segment in segments:
        fixed_segments.append(
            Segment(label=segment[2], start=fix_time(shifts, segment[0].start), end=fix_time(shifts, segment[0].end)))

    fixed_segments = cut_non_speech_regions(fixed_segments, speech_segments)
    for seg in fixed_segments:
        res[PyannoteSegment(seg.start, seg.end)] = seg.label

    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio diarizatiopn")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")
    segments = load_segments(args.input)
    speech_segments = select_speech(segments)
    res = []
    if len(speech_segments) > 0:
        res = diarization(args.input, speech_segments)
        logger.info(f"Got results: {res}")

    if not isinstance(res, list):
        res = restore_time(res, speech_segments)

    with open(args.output, "w") as f:
        if not isinstance(res, list):
            res.write_rttm(f)
        else:
            logger.warn("Got empty result")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
