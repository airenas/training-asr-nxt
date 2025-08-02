import os

import torchaudio

from preparation.logger import logger
from preparation.utils.audio import extracted_audio_to_buffer
from preparation.utils.sm_segments import Segment


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


class SpeakerLangRes:
    def __init__(self, speaker, lang_res):
        self.speaker = speaker
        self.lang_res = lang_res

    def to_dict(self):
        return {
            "speaker": self.speaker,
            "lang_res": [res.to_dict() for res in self.lang_res]
        }

    def __repr__(self):
        lang_res_str = ', '.join([str(res) for res in self.lang_res])
        return f"SpeakerLangRes(speaker={self.speaker}, lang_res=[{lang_res_str}])"


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


def get_device_str() -> str:
    val = os.environ.get("CUDA_VISIBLE_DEVICES")
    if val is not None and val == "":
        return "cpu"
    return "cuda"


def detect_language(audio_path, segments):
    from speechbrain.inference.classifiers import EncoderClassifier
    device = get_device_str()
    logger.debug(f"Device {device}")

    language_id = EncoderClassifier.from_hparams(
        source="speechbrain/lang-id-voxlingua107-ecapa",
        savedir="tmp/lang-id",
        run_opts = {"device": device}
    )

    waveform, sample_rate = torchaudio.load(audio_path)
    res = []
    for segment in segments:
        part = extracted_audio_to_buffer(waveform, sample_rate, segment)

        lang, conf = detect_lang(language_id, part)
        if lang:
            logger.info(f"Detected language: {lang} with confidence {conf}")
            res += [
                LangRes(segment=Segment(start=segment.start, end=segment.end), lang=lang.split(":")[0], conf=conf)]
    return res
