import torchaudio

class Audio:
    def __init__(self, waveform, sample_rate):
        self.waveform = waveform
        self.sample_rate = sample_rate

    def cut(self, segments) -> "Audio":
        """
        Cut the audio into segments.
        :param segments: list of tuples (start, end) in seconds
        :return: list of Audio objects
        """
        import torch

        combined_waveform = []
        for segment in segments:
            start_sample = int(segment.start * self.sample_rate)
            end_sample = int(segment.end * self.sample_rate)
            part = self.waveform[:, start_sample:end_sample]
            combined_waveform.append(part)

        res = torch.cat(combined_waveform, dim=1)
        return Audio(res, self.sample_rate)

    @classmethod
    def from_file(cls, file_str):
        waveform, sample_rate = torchaudio.load(file_str)
        return Audio(waveform, sample_rate)
