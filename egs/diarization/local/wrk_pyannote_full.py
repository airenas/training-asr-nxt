import json
import os
import sys

from preparation.logger import logger
from preparation.utils.audio import Audio


def init_model():
    from pyannote.audio import Pipeline
    import torch

    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token=os.getenv('HF_API_TOKEN'))

    pipeline.to(torch.device("cuda"))
    return pipeline


def diarization(model, audio_path):
    audio = Audio.from_file(audio_path)

    res = model({"waveform": audio.waveform, "sample_rate": audio.sample_rate})
    return res


def main(argv):
    logger.info("Starting")

    model = init_model()

    for line in sys.stdin:
        try:
            task = json.loads(line)
            logger.info(f"Input wav file   : {task.get('input_wav')}")
            logger.info(f"Output file  : {task.get('output')}")
            res = diarization(model, task.get('input_wav'))
            logger.info(f"Got results: {res}")

            with open(task.get('output'), "w") as f:
                if not isinstance(res, list):
                    res.write_rttm(f)

                else:
                    logger.warn("Got empty result")
            print("\nwrk-res: ok", flush=True)
        except Exception as e:
            logger.error(f"Failed to process line {line}: {e}")
            print(f"\nwrk-res: error: {str(e)}", flush=True)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
