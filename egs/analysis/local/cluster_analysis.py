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
    logger.info(f"start get audio bytes for segments")
    response = requests.post(
        url,
        json=segments,
        timeout=10
    )

    response.raise_for_status()
    logger.info(f"got audio bytes: {len(response.content)}")
    return response.content


class Speaker:
    data: list | None = None
    index: int = 0
    start: int = 0

    def __init__(self, index, data):
        self.index = index
        self.data = data
        self.start = 0


@dataclass
class ViewItem:
    index: int
    start: int
    audio: bytes | None


def get_labeled(data, old_labels):
    for item in data:
        key = (item["f"], item["sp"])
        if key in old_labels:
            return old_labels[key]
    return None


class App:
    def __init__(self, args):
        self.args = args
        self.old_labels = {}
        self.current: Speaker | None = None
        self.iterator = self._iterate_segments()

    def _iterate_segments(self):
        with open(self.args.input, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if not line.strip():
                    continue
                if i < self.args.skip_input:
                    continue
                data = json.loads(line)
                yield Speaker(index=i, data=data)

    def get_next(self) -> Speaker | None:
        self.current = next(self.iterator, None)
        return self.current



def make_description(sp: Speaker) -> str:
    return f"{sp.index} | {json.dumps(sp.data[0:2], ensure_ascii=False)}({len(sp.data)})"


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Label speaker gender GUI")
    parser.add_argument("--input", nargs='?', required=True, help="Clustered file")
    parser.add_argument("--audio-url", nargs='?', required=True, help="Audio segments provider url")
    parser.add_argument("--prefetch", type=int, default=3, help="How many next items to prefetch in background")
    parser.add_argument("--skip-input", nargs='?', default=0, type=int,
                        help="How many items to skip from input (for debugging)")

    args = parser.parse_args(args=argv)

    logger.info(f"Input file   : {args.input}")

    db_url = os.environ["DB_URL"]
    logger.info("iterate starting")
    with psycopg2.connect(dsn=db_url) as conn:

        l_app = App(args)

        def empty_item():
            return "", 0, "-"

        def to_view_item(sp: Speaker | None, start: int):
            logger.info(f"to view item for speaker {sp.index if sp else None} from {start}")
            if sp is None:
                return empty_item()
            sp.start = start
            segments = make_segments(conn=conn, data=sp.data, from_start=start, max_segments=50)
            audio_b = get_audio_bytes(args.audio_url, segments)
            item = ViewItem(
                index=sp.index,
                start=start,
                audio=audio_b,
            )
            return make_description(sp), item.start, item.audio, item

        def get_item():
            sp = l_app.get_next()
            return to_view_item(sp, 0)

        def load_audio(item: ViewItem | None, _from: int = 0):
            sp = l_app.current
            if sp is None:
                return empty_item()
            return to_view_item(sp, _from)

        with gr.Blocks() as app:
            gr.Markdown("# 🎧 Speaker Clusters")

            current_item = gr.State(value=None)

            description = gr.Textbox(label="Description", interactive=False)
            start_from = gr.Textbox(label="Audio From", interactive=True)
            audio = gr.Audio(autoplay=True)

            with gr.Row():
                btn_load = gr.Button("Load Audio")
                btn_bext_speaker = gr.Button("Next Speaker")

            def load():
                return get_item()

            app.load(fn=load, outputs=[description, start_from, audio, current_item])

            btn_load.click(
                fn=lambda item, _from: load_audio(item, int(_from)),
                inputs=[current_item, start_from],
                outputs=[description, start_from, audio, current_item],
            )
            btn_bext_speaker.click(
                fn=lambda item: get_item(),
                outputs=[description, start_from, audio, current_item],
            )

        app.launch()

    logger.info(f"Done")


def gender_to_str(gender: Gender | None):
    if gender == Gender.MALE:
        return "m"
    elif gender == Gender.FEMALE:
        return "f"
    return '-'


def make_segments(conn, data, min_segment_duration: float = 1.0, from_start: int = 0,  max_segments: int = 10):
    logger.info(f"start make segments")
    res = []
    for item in data[from_start:]:
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
