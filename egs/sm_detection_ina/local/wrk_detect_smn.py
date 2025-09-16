import json
import sys

from preparation.cache.file import Params
from preparation.logger import logger


def init_model():
    from inaSpeechSegmenter import Segmenter
    pipeline = Segmenter(vad_engine="smn", detect_gender=False, ffmpeg="ffmpeg", batch_size=1)
    return pipeline


def main(argv):
    logger.info("Starting")

    model = init_model()

    for line in sys.stdin:
        try:
            task = json.loads(line)
            logger.info(f"Input wav file   : {task.get('input_wav')}")
            logger.info(f"Output file  : {task.get('output')}")

            res = model(task.get('input_wav'))
            logger.info(f"Got segments: {res}")

            with open(task.get('output'), "w") as f:
                for segment in res:
                    f.write(Params.to_json_str(segment))
            logger.info("written all segments")
            print("\nwrk-res: ok", flush=True)
        except Exception as e:
            logger.error(f"Failed to process line {line}: {e}")
            print(f"\nwrk-res: error: {str(e)}", flush=True)

    logger.info(f"Done")


if __name__ == "__main__":
    main(sys.argv[1:])
