import logging

import numpy as np
import xgboost as xgb

from egs.gender.local.gender import Gender

def to_number(g: Gender):
    if g == Gender.FEMALE:
        return 0
    elif g == Gender.MALE:
        return 1
    else:
        return 0.5


class Model:
    def __init__(self, file: str):
        self.model = Model.load(file)

    @classmethod
    def load(cls, path: str) -> "xgb.XGBClassifier":
        logging.info(f"Loading model from {path}")
        model = xgb.XGBClassifier()
        model.load_model(path)
        return model

    def _predict_prob(self, emb: list[float]) -> float:
        x = np.array(emb, dtype=np.float32).reshape(1, -1)
        prob = self.model.predict_proba(x)[0][1]  # probability
        return float(prob)

    def predict(self, emb: list[float], threshold_diff: float = 0.0) -> Gender:
        prob = self._predict_prob(emb)
        if prob >= 0.5 + threshold_diff:
            return Gender.MALE
        elif prob <= 0.5 - threshold_diff:
            return Gender.FEMALE
        return Gender.UNK
