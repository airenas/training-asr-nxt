import os

from pyannote.core import Annotation, Timeline

from preparation.utils.sm_segments import Segment


def load_rttm(input_file, file_name="audio.rttm"):
    dir_name = os.path.dirname(input_file)
    res_file = os.path.join(dir_name, file_name)

    from pyannote.database.util import load_rttm

    return load_rttm(res_file).get('waveform', [])


def remove_overlaps(annotation: Annotation) -> Annotation:
    if len(annotation) == 0:
        return annotation
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


def take_middle(segment: Segment, min_segment_duration: float) -> Segment:
    """
    Take the middle part of the segment
    :param min_segment_duration: Minimum duration of the segment to consider
    :return: Segment object with adjusted start and end times
    """
    if segment.end - segment.start < min_segment_duration:
        return segment

    start = (segment.start + segment.end) / 2 - min_segment_duration / 2
    end = start + min_segment_duration
    return Segment(start=start, end=end, label=segment.label)
