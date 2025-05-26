import argparse
import io
import json
import sys

import torchaudio

from preparation.logger import logger
from preparation.utils.sm_segments import Segment, load_segments


def detect_lang(language_id, audio_buffer):
    try:
        # signal = language_id.load_audio(audio_path)
        signal, sample_rate = torchaudio.load(audio_buffer)
        prediction = language_id.classify_batch(signal)
        _, log_prob_target, _, predicted_lang = prediction
        predicted = predicted_lang[0]
        confidence = log_prob_target.exp().item()
        return predicted, confidence
    except Exception as e:
        logger.error(f"{audio_buffer}: ERROR - {e}")
        return None, 0.0


def extracted_audio(waveform, sample_rate, segment):
    start_sample = int(segment.start * sample_rate)
    end_sample = int(segment.end * sample_rate)
    part = waveform[:, start_sample:end_sample]

    buffer = io.BytesIO()
    torchaudio.save(buffer, part, sample_rate, format="wav")
    buffer.seek(0)
    return buffer


def detect_language(audio_path, segments):
    from speechbrain.inference.classifiers import EncoderClassifier

    language_id = EncoderClassifier.from_hparams(
        source="speechbrain/lang-id-voxlingua107-ecapa",
        savedir="tmp/lang-id"
    )

    waveform, sample_rate = torchaudio.load(audio_path)
    res = []
    for segment in segments:
        part = extracted_audio(waveform, sample_rate, segment)

        lang, conf = detect_lang(language_id, part)
        if lang:
            logger.info(f"Detected language: {lang} with confidence {conf}")
            res += [LangRes(segment=segment, lang=lang.split(":")[0], conf=conf)]
    return res


class LangRes:
    def __init__(self, segment, lang, conf):
        self.segment = segment
        self.lang = lang
        self.conf = conf

    def to_dict(self):
        return {
            "segment": self.segment.to_dict(),
            "lang": self.lang,
            "conf": self.conf
        }

    def __repr__(self):
        return f"LangRes(segment={self.segment}, lang={self.lang}, conf={self.conf})"


def find_segment(speech_segments, start, min_len):
    to = start + min_len
    for s in speech_segments:
        if s.duration < min_len:
            continue
        if s.start >= start:
            return Segment(s.label, s.start, s.start + min_len)
        if s.end >= to:
            return Segment(s.label, s.end - min_len, s.end)
        if s.start < start and s.end > to:
            return Segment(s.label, start, to)
    return None


def select_test_segments(segments, num_segments=5, min_len=5):
    speech_segments = [seg for seg in segments if seg.label == SegmentLabel.SPEECH.value]
    total_duration = sum(s.end - s.start for s in speech_segments)
    logger.info(f"total speech duration: {total_duration}")
    parts_from = total_duration / num_segments + 1
    res = []
    for i in range(num_segments):
        start = i * parts_from
        s = find_segment(speech_segments, start, min_len)
        if s:
            if s not in res:
                res.append(s)
    return res


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Runs audio processing")
    parser.add_argument("--input", nargs='?', required=True, help="File")
    parser.add_argument("--output", nargs='?', required=True, help="File")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")
    segments = load_segments(args.input)
    # logger.info(f"Got segments: {segments}")
    for sl in [5, 3, 1]:
        test_segments = select_test_segments(segments, num_segments=5, min_len=sl)
        if len(test_segments) > 1:
            break
    res = []
    if len(test_segments) > 0:
        logger.info(f"Made test segments: {test_segments}")
        res = detect_language(args.input, test_segments)
        logger.info(f"Got results: {res}")

    with open(args.output, "w") as f:
        for r in res:
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")
    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
