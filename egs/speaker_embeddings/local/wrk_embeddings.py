import io
import json
import random
import sys

import torchaudio

from preparation.logger import logger
from preparation.utils.audio import Audio
from preparation.utils.rttm import load_rttm, remove_overlaps
from preparation.utils.sm_segments import Segment


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

    def get_audio(self):
        if self.audio is None:
            self.audio = Audio.from_file(self.file)
        return self.audio


def init_model(gpu: bool = True):
    from pyannote.audio import Model
    model = Model.from_pretrained("pyannote/wespeaker-voxceleb-resnet34-LM")
    from pyannote.audio import Inference
    inference = Inference(model, window="whole")
    if gpu:
        import torch
        inference.to(torch.device("cuda"))
        model = inference
    return model


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


def calc_embedding(model, params: Params, segments):
    audio = params.get_audio()
    speaker_audio = audio.cut(segments)
    res = model({"waveform": speaker_audio.waveform, "sample_rate": speaker_audio.sample_rate})
    return res


def get_wanted_min_segment_len(params):
    audio = params.get_audio()
    duration = audio.waveform.shape[1] / audio.sample_rate
    if duration < 30:
        return 1.0
    return 3.0


def main(argv):
    logger.info("Starting")

    model = init_model(gpu=True)

    for line in sys.stdin:
        try:
            task = json.loads(line)

            wav_file = task.get('input_wav')
            rttm_file = task.get('input')
            output_file = task.get('output')

            logger.info(f"Input wav file   : {wav_file}")
            logger.info(f"Input rttm file   : {rttm_file}")
            logger.info(f"Output file  : {output_file}")

            params = Params(wav_file)
            annotations = load_rttm(rttm_file)
            annotations = remove_overlaps(annotations)
            res = {}
            if len(annotations) > 0:
                logger.info(f"loaded rttm lines: {len(annotations)}")
                speakers = set(annotations.labels())
                logger.info(f"loaded speakers: {len(speakers)}")
                min_segment_len = get_wanted_min_segment_len(params)
                logger.info(f"min segment len for embedding: {min_segment_len:.2f}s")
                for speaker in speakers:
                    logger.info(f"Speaker: {speaker}")
                    segments = get_speaker_segments_for_embedding(annotations, speaker, min_segment_duration=min_segment_len)
                    if len(segments) > 0:
                        logger.info(f"Speaker {speaker} segments: {len(segments)}")
                        sp_emb = calc_embedding(model, params, segments)
                        res[speaker] = sp_emb
            if len(res) == 0:
                logger.warning(f"No embeddings calculated for rttm file {rttm_file}")
            with open(output_file, "w") as f:
                for k, v in res.items():
                    f.write(json.dumps({"sp": k, "emb": v.tolist()}, ensure_ascii=False) + "\n")

            print("\nwrk-res: ok", flush=True)
        except Exception as e:
            logger.error(f"Failed to process line {line}: {e}")
            print(f"\nwrk-res: error: {str(e)}", flush=True)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
