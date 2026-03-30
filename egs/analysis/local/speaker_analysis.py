import argparse
import json
import os
import sys
from dataclasses import dataclass

import gradio as gr
import psycopg2
import requests

from egs.analysis.local.speaker import load_rttm
from preparation.logger import logger


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


@dataclass
class ViewItem:
    speakers: str
    segments: str
    range: str

    def __init__(self, speakers="", segments=""):
        self.speakers = speakers
        self.segments = segments
        self.range = "0:20"


class App:
    def __init__(self, conn, audio_url):
        self.conn = conn
        self.audio_url = audio_url

    def load_segments(self, speakers: str):
        res = []
        for line in speakers.split("\n"):
            line = line.strip()
            if not line.strip():
                continue
            cols = line.split("|")
            if len(cols) < 2:
                continue
            file, speaker = cols[0].strip(), cols[1].strip()
            segments = make_segments(conn=self.conn, f=file, speaker=speaker)
            res.extend(segments)
        return json.dumps(res, indent=2)

    def load_audio(self, segments, rng):
        selected = segments[rng._from: rng._to]
        logger.info(f"selected segments: {len(selected)}")
        return get_audio_bytes(url=self.audio_url, segments=selected)


class Range:
    _from: int
    _to: int

    def __init__(self, _from, _to):
        self._from = _from
        self._to = _to


def parse_range(range):
    rng = range.split(":")
    if len(rng) != 2:
        raise ValueError(f"invalid range {range}")
    try:
        _from = int(rng[0].strip())
        _to = int(rng[1].strip())
    except Exception as e:
        raise ValueError(f"invalid range {range}: {e}")
    if _from < 0 or _to < 0:
        raise ValueError(f"invalid range {range}: from and to should be non negative")
    if _from >= _to:
        raise ValueError(f"invalid range {range}: from should be less than to")
    return Range(_from=_from, _to=_to)


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Speaker analysis GUI")
    parser.add_argument("--audio-url", nargs='?', required=True, help="Audio segments provider url")

    args = parser.parse_args(args=argv)

    db_url = os.environ["DB_URL"]
    with psycopg2.connect(dsn=db_url) as conn:

        l_app = App(conn=conn, audio_url=args.audio_url)

        def to_view(v: ViewItem, _audio: bytes | None):
            return v.speakers, v.segments, v.range, _audio, v

        def load_audio(v: ViewItem, _segments: str, _range: str):
            if _segments is None:
                return to_view(v=v, _audio=None)
            if _range is None:
                return to_view(v=v, _audio=None)
            v.segments = _segments
            v.range = _range
            segments_json = json.loads(_segments)
            rng = parse_range(v.range)
            audio = l_app.load_audio(segments_json, rng)
            return to_view(v=v, _audio=audio)

        def load_segments(v: ViewItem, _speakers: str):
            v.speakers = _speakers
            v.segments = l_app.load_segments(v.speakers)
            return to_view(v=v, _audio=None)

        with gr.Blocks() as app:
            gr.Markdown("# 🎧 Speakers")

            current_item = gr.State(value=None)

            speaker = gr.Textbox(label="Speaker", interactive=True)
            segments = gr.Textbox(label="Segments", interactive=True)
            range_comp = gr.Textbox(label="Range", interactive=True)
            audio = gr.Audio(autoplay=True)

            with gr.Row():
                btn_load_segments = gr.Button("Load Segments")
                btn_load_audio = gr.Button("Load Audio")

            def load():
                return to_view(v=ViewItem(), _audio=None)

            app.load(fn=load, outputs=[speaker, segments, range_comp, audio, current_item])

            btn_load_segments.click(
                fn=lambda v, _speaker: load_segments(v, _speaker),
                inputs=[current_item, speaker],
                outputs=[speaker, segments, range_comp, audio, current_item],
            )
            btn_load_audio.click(
                fn=lambda v, _segments, _range: load_audio(v, _segments, _range),
                inputs=[current_item, segments, range_comp],
                outputs=[speaker, segments, range_comp, audio, current_item],
            )

        app.launch()

    logger.info(f"Done")


def make_segments(conn, f, speaker):
    logger.info(f"start make segments")
    res = []
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
    for s in segments:
        entry = {
            "file": f"{path}/audio.16.wav",
            "from": s["from"],
            "to": s["to"],
        }
        res.append(entry)
    return res


if __name__ in {"__main__", "__mp_main__"}:
    main(sys.argv[1:])
