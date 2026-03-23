import argparse
import json
import os
import queue
import sys
import threading
from dataclasses import dataclass

import gradio as gr
import psycopg2
import requests

from egs.analysis.local.speaker import load_rttm
from egs.gender.local.gender import Gender
from egs.gender.local.model import Model
from preparation.logger import logger
from preparation.metadata.embedding import get_speaker_embedding


def get_audio_bytes(url, segments):
    response = requests.post(
        url,
        json=segments,
    )

    response.raise_for_status()
    return response.content


class Speaker:
    data: list | None = None
    segments: list | None = None
    audio: bytes | None = None
    index: int = 0

    def __init__(self, index, data, segments, audio, predicted_label: Gender = None, real_label: Gender = None):
        self.index = index
        self.data = data
        self.segments = segments
        self.audio = audio
        self.predicted_label = predicted_label
        self.real_label = real_label


@dataclass
class ViewItem:
    index: int
    title: str
    predicted: str
    real: str
    audio: bytes | None
    data: list


def get_labeled(data, old_labels):
    for item in data:
        key = (item["f"], item["sp"])
        if key in old_labels:
            return old_labels[key]
    return None


class App:
    _END = object()

    def __init__(self, args):
        self.args = args
        self._done = False
        self._producer_error = None
        prefetch = max(1, args.prefetch)
        self._queue = queue.Queue(maxsize=prefetch)
        if os.path.exists(args.model):
            self.model = Model(args.model)
        self.old_labels = {}
        open_mode = "w"
        if os.path.exists(args.output):
            # load existing labels
            logger.info(f"Output file {args.output} already exists, loading existing labels")
            with open(args.output, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    key = (data["f"], data["sp"])
                    self.old_labels[key] = data
        if self.old_labels:
            logger.info(f"Loaded {len(self.old_labels)} existing labels")
            open_mode = "a"
        self.output_file = args.output
        self.output_f = open(self.output_file, open_mode, encoding="utf-8")

        self._producer = threading.Thread(target=self._prefetch_loop, daemon=True)
        self._producer.start()

    def _prefetch_loop(self):
        try:
            for item in self._iterate_segments():
                self._queue.put(item)
        except Exception as exc:
            self._producer_error = exc
        finally:
            self._queue.put(self._END)

    def _iterate_segments(self):
        db_url = os.environ["DB_URL"]
        logger.info("iterate starting")
        with psycopg2.connect(dsn=db_url) as conn:
            with open(self.args.input, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if not line.strip():
                        continue
                    if i < self.args.skip_input:
                        # logger.info(f"{i}: skipping")
                        continue
                    data = json.loads(line)
                    # logger.info(f"{i}: got {len(data)} records")

                    labeled = get_labeled(data, self.old_labels)
                    real_label = None
                    if labeled:
                        real_label = Gender.MALE if labeled.get("label") == "m" else Gender.FEMALE

                    segments = make_segments(conn, data, min_segment_duration=3, max_segments=10)
                    if not segments:
                        # logger.info(f"{i}: no segments, skipping")
                        continue
                    pred = None
                    if self.model:
                        emb = get_speaker_embedding(conn, data[0]["f"], data[0]["sp"])
                        pred = self.model.predict(emb)
                        logger.info(f"{i}: predicted label {pred}")

                    if pred and pred == real_label:
                        # logger.info(f"{i}: predicted label matches real label, skipping")
                        continue

                    audio_bytes = get_audio_bytes(self.args.audio_url, segments)
                    # logger.info(f"{i}: got {len(audio_bytes)} audio")

                    yield Speaker(index=i, data=data, segments=segments, audio=audio_bytes, predicted_label=pred,
                                  real_label=real_label)

    def get_next(self):
        if self._done:
            return None
        item = self._queue.get()
        if item is self._END:
            self._done = True
            if self._producer_error:
                raise self._producer_error
            return None
        return item

    def save_label(self, data, value):
        for item in data:
            item["label"] = value
            logger.info(f"Saving label {value} for file {item['f']} speaker {item['sp']}")
            self.output_f.write(json.dumps(item, ensure_ascii=False) + "\n")
        self.output_f.flush()


def make_description(sp: Speaker) -> str:
    return f"{sp.index} | {json.dumps(sp.data[0], ensure_ascii=False)}({len(sp.data)})"


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Label speaker gender GUI")
    parser.add_argument("--input", nargs='?', required=True, help="Clustered file")
    parser.add_argument("--audio-url", nargs='?', required=True, help="Audio segments provider url")
    parser.add_argument("--model", nargs='?', required=True,
                        help="Model file for prediction, used to sort items by confidence")
    parser.add_argument("--prefetch", type=int, default=3, help="How many next items to prefetch in background")
    parser.add_argument("--output", nargs='?', required=True, help="File")
    parser.add_argument("--skip-input", nargs='?', default=0, type=int,
                        help="How many items to skip from input (for debugging)")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")
    logger.info(f"Output file  : {args.output}")
    logger.info(f"Model file   : {args.model}")

    l_app = App(args)

    def empty_item():
        return "", "DONE", "-", "-", None, None

    def to_view_item(sp: Speaker | None):
        if sp is None:
            return empty_item()
        item = ViewItem(
            index=sp.index,
            title=json.dumps(sp.data[0], ensure_ascii=False),
            predicted=gender_to_str(sp.predicted_label),
            real=gender_to_str(sp.real_label),
            audio=sp.audio,
            data=sp.data[:10],
        )
        pred_str = f"{sp.predicted_label.name}"
        if sp.real_label == sp.predicted_label:
            pred_str += " (ok)"
        else:
            if item.real != '-':
                pred_str += " (MISMATCH)"
        return make_description(sp), pred_str, item.audio, item

    def get_item():
        sp = l_app.get_next()
        return to_view_item(sp)

    def save_and_next(item: ViewItem | None, label: str | None):
        if item is not None and label:
            logger.info(f"Saving label {label} for item {item.index}")
            l_app.save_label(item.data, label)
        return get_item()

    with gr.Blocks() as app:
        gr.Markdown("# 🎧 Speaker Gender Labeling")

        current_item = gr.State(value=None)

        description = gr.Textbox(label="Description", interactive=False)
        pred_real = gr.Textbox(label="Prediction", interactive=False)
        audio = gr.Audio(autoplay=True)

        with gr.Row():
            btn_m = gr.Button("Male")
            btn_f = gr.Button("Female")
            btn_skip = gr.Button("Skip")

        def load():
            return get_item()

        app.load(fn=load, outputs=[description, pred_real, audio, current_item])

        btn_m.click(
            fn=lambda item: save_and_next(item, "m"),
            inputs=[current_item],
            outputs=[description, pred_real, audio, current_item],
        )
        btn_f.click(
            fn=lambda item: save_and_next(item, "f"),
            inputs=[current_item],
            outputs=[description, pred_real, audio, current_item],
        )
        btn_skip.click(
            fn=lambda item: save_and_next(item, None),
            inputs=[current_item],
            outputs=[description, pred_real, audio, current_item],
        )

    app.launch()

    logger.info(f"Done")


def gender_to_str(gender: Gender | None):
    if gender == Gender.MALE:
        return "m"
    elif gender == Gender.FEMALE:
        return "f"
    return '-'


def make_segments(conn, data, min_segment_duration: float = 1.0, max_segments: int = 10):
    res = []
    for item in data:
        f = item.get("f")
        speaker = item.get("sp")
        # logger.info(f"Processing file id {f} speaker {speaker}")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, path FROM files WHERE id = %s",
                (f,)
            )
            row = cur.fetchone()
        if not row:
            raise ValueError(f"No file id {f}")
        path = row[1]
        rttm = load_rttm(conn, f)
        segments = rttm.get(speaker, [])
        if not segments:
            raise ValueError(f"No segment for file id {f} speaker {speaker}")
        duration_f = 0
        for s in segments:
            d = s["to"] - s["from"]
            if d < min_segment_duration:
                # logger.info(f"Skipping short segment {d:.2f}s for file id {f} speaker {speaker}")
                continue
            entry = {
                "file": f"{path}/audio.16.wav",
                "from": s["from"],
                "to": s["to"],
            }
            duration_f += s["to"] - s["from"]
            res.append(entry)
            if len(res) >= max_segments:
                return res
    return res


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
