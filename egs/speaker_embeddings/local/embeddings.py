import argparse
import io
import json
import os
import sys
import random

import torchaudio
from pyannote.core import Annotation, Timeline

from preparation.logger import logger
from preparation.utils.audio import Audio
from preparation.utils.sm_segments import Segment


def load_rttm(input_file):
    dir_name = os.path.dirname(input_file)
    res_file = os.path.join(dir_name, "audio.rttm")

    from pyannote.database.util import load_rttm

    return load_rttm(res_file).get('waveform', [])


def extracted_audio(waveform, sample_rate, segment):
    start_sample = int(segment.start * sample_rate)
    end_sample = int(segment.end * sample_rate)
    part = waveform[:, start_sample:end_sample]

    buffer = io.BytesIO()
    torchaudio.save(buffer, part, sample_rate, format="wav")
    buffer.seek(0)
    return buffer


class Params:
    def __init__(self, file):
        self.file = file
        self.audio = None
        self.model = None
        self.gpu = False

    def get_model(self):
        if self.model is None:
            from pyannote.audio import Model
            model = Model.from_pretrained("pyannote/wespeaker-voxceleb-resnet34-LM")
            from pyannote.audio import Inference
            inference = Inference(model, window="whole")
            if self.gpu:
                import torch
                inference.to(torch.device("cuda"))
            self.model = inference
        return self.model

    def get_audio(self):
        if self.audio is None:
            self.audio = Audio.from_file(self.file)
        return self.audio


def remove_overlaps(annotation: Annotation, speaker) -> Annotation:
    overlap = annotation.get_overlap()
    cleaned = Annotation()

    for speaker in annotation.labels():
        speaker_segments = Timeline([
            segment for segment, _, label in annotation.itertracks(yield_label=True)
            if label == speaker
        ])
        trimmed = speaker_segments.extrude(overlap, mode='intersection')
        for segment in trimmed:
            cleaned[segment] = speaker

    return cleaned


def chop_segment(segment: Segment, chunk_duration: float) -> list[Segment]:
    s = Segment(start=segment.start, end=segment.end, label=segment.label)
    res = []
    slice_chunk_min = (chunk_duration * 0.4) + chunk_duration

    while s.duration > slice_chunk_min:
        res.append(Segment(start=s.start, end=s.start + chunk_duration, label=s.label))
        s.start += chunk_duration
    res.append(s)
    return res


def get_speaker_segments_for_embedding(annotation, speaker: str,
                                       max_total_duration: float = 120.0,
                                       min_segment_duration: float = 3.0,
                                       slice_chunk_duration: float = 20.0) -> list[Segment]:
    annotation = remove_overlaps(annotation, speaker)
    segments = [Segment(start=s.start, end=s.end, label=speaker) for s, _, label in
                annotation.itertracks(yield_label=True) if
                label == speaker]

    segments = [s for s in segments if s.duration >= min_segment_duration]

    all_chunks = []
    for segment in segments:
        all_chunks.extend(chop_segment(segment, chunk_duration=slice_chunk_duration))

    random.Random(42).shuffle(all_chunks)

    res = []
    total = 0.0
    for seg in all_chunks:
        res.append(seg)
        total += seg.duration
        if total >= max_total_duration:
            break
    res = sorted(res, key=lambda s: s.start)
    return res


def calc_embedding(params: Params, segments):
    audio = params.get_audio()
    speaker_audio = audio.cut(segments)
    model = params.get_model()
    res = model({"waveform": speaker_audio.waveform, "sample_rate": speaker_audio.sample_rate})
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Extracts speaker embeddings from a file")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")
    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")

    dir_name = os.path.dirname(args.input)

    params = Params(os.path.join(dir_name, "audio.16.wav"))
    annotations = load_rttm(args.input)
    res = {}
    if len(annotations) > 0:
        logger.info(f"loaded rttm lines: {len(annotations)}")
        speakers = set(annotations.labels())
        logger.info(f"loaded speakers: {len(speakers)}")
        for sp in speakers:
            logger.info(f"Speaker: {sp}")
            segments = get_speaker_segments_for_embedding(annotations, sp)
            if len(segments) > 0:
                logger.info(f"Speaker {sp} segments: {len(segments)}")
                sp_emb = calc_embedding(params, segments)
                res[sp] = sp_emb

    with open(args.output, "w") as f:
        for k, v in res.items():
            f.write(json.dumps({"sp": k, "emb": v.tolist()}, ensure_ascii=False) + "\n")

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
