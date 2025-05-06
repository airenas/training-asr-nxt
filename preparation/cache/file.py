import json
import os

from preparation.logger import logger


class Params:
    def __init__(self, input_dir, output_dir, name, run_f, deserialize_func, serialize_func=str):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.run_f = run_f
        self.deserialize_func = deserialize_func
        self.serialize_func = serialize_func
        self.name = name

    @classmethod
    def to_int(cls, s: str) -> int:
        return int(s)

    @classmethod
    def to_float(cls, s: str) -> float:
        return float(s)

    @classmethod
    def to_json(cls, s: str) -> dict:
        return json.loads(s)

    @classmethod
    def to_json_str(cls, s) -> str:
        return json.dumps(s)


def file_cache_func(file_path: str, params: Params):
    relative_path = os.path.relpath(file_path, params.input_dir)
    output_file_dir = os.path.join(params.output_dir, relative_path)
    cache_file_path = os.path.join(output_file_dir, params.name)

    logger.debug(f"Processing file: {file_path}, output: {cache_file_path}")

    res, err = None, None
    try:
        os.makedirs(output_file_dir, exist_ok=True)

        if os.path.exists(cache_file_path):
            logger.debug(f"Found cache: {cache_file_path}")
            with open(cache_file_path, "r") as f:
                res = params.deserialize_func(f.read().strip())
        else:
            logger.debug(f"Not found cache: {cache_file_path}")
            res = params.run_f(file_path)
            logger.debug(f"Creating: {cache_file_path}")
            with open(cache_file_path, "w") as f:
                f.write(params.serialize_func(res))
    except Exception as e:
        err = e

    if err:
        return {"file_path": file_path, "error": str(err), "ok": False}
    return {"file_path": file_path, "result": res, "ok": True}
