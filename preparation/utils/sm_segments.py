import math
import os
import re
from enum import Enum


class SegmentLabel(Enum):
    SPEECH = "speech"
    MUSIC = "music"
    NO_ENERGY = "noEnergy"
    NOISE = "noise"


class Segment:
    def __init__(self, label: str, start: float, end: float):
        self.label = label
        self.start = start
        self.end = end

    def __repr__(self):
        return f"Segment(label={self.label}, start={self.start}, end={self.end})"

    @property
    def duration(self):
        return self.end - self.start

    def __eq__(self, other):
        if not isinstance(other, Segment):
            return False
        return self.label == other.label and math.isclose(self.start, other.start) and math.isclose(self.end, other.end)

    def to_dict(self):
        return {
            "label": self.label,
            "start": self.start,
            "end": self.end
        }


def load_segments(input_file):
    dir_name = os.path.dirname(input_file)
    segments_file = os.path.join(dir_name, "audio.ina_segments")

    try:
        with open(segments_file, 'r', encoding='utf-8') as f:
            content = f.read()
        segments = re.findall(r'\["(.*?)", ([\d.]+), ([\d.]+)\]', content)
        parsed_segments = [Segment(label, float(start), float(end)) for label, start, end in segments]
        return parsed_segments
    except Exception as e:
        raise RuntimeError(f"Failed to load segments from {segments_file}: {e}")


def select_speech(segments):
    return [seg for seg in segments if seg.label == SegmentLabel.SPEECH.value]
