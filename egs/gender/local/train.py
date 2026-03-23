import argparse
import json
import sys
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import optuna
import xgboost as xgb
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from egs.gender.local.gender import Gender
from egs.gender.local.model import to_number
from preparation.logger import logger


def load_jsonl(path: str) -> Tuple[np.ndarray, np.ndarray]:
    X, y = [], []

    with open(path, "r") as f:
        for line in f:
            obj = json.loads(line)
            X.append(obj["emb"])
            y.append(to_number(Gender.from_str(obj["g"])))

    return np.array(X, dtype=np.float32), np.array(y)


@dataclass
class TrainConfig:
    test_size: float = 0.2
    random_state: int = 42
    n_trials: int = 50


def make_objective(
        X_train: np.ndarray,
        X_val: np.ndarray,
        y_train: np.ndarray,
        y_val: np.ndarray,
):
    def objective(trial: optuna.Trial) -> float:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 200, 800),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
            "min_child_weight": trial.suggest_float("min_child_weight", 1, 10),
            "gamma": trial.suggest_float("gamma", 0, 5),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10, log=True),
            "eval_metric": "logloss",
            "tree_method": "hist",
            "random_state": 42,
        }

        model = xgb.XGBClassifier(**params)

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_val, y_val)],
            # early_stopping_rounds=30,
            verbose=False,
        )

        y_prob = model.predict_proba(X_val)[:, 1]
        return roc_auc_score(y_val, y_prob)

    return objective


def train(path: str, config: TrainConfig) -> xgb.XGBClassifier:
    X, y = load_jsonl(path)

    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=y,
    )
    logger.info(f"split data: {len(X_train)} train samples, {len(X_val)} val samples")
    ## statistics male/female
    logger.info(f"train set: {int(np.sum(y_train))} male, {int(len(y_train) - np.sum(y_train))} female")
    logger.info(f"val set: {int(np.sum(y_val))} male, {int(len(y_val) - np.sum(y_val))} female")

    study = optuna.create_study(direction="maximize")

    objective = make_objective(X_train, X_val, y_train, y_val)

    study.optimize(objective, n_trials=config.n_trials)

    best_params = study.best_params

    # Final model (retrain clean)
    final_model = xgb.XGBClassifier(
        **best_params,
        eval_metric="logloss",
        tree_method="hist",
        random_state=config.random_state,
    )

    final_model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        # early_stopping_rounds=30,
        verbose=False,
    )

    return final_model


def main(argv):
    logger.info("Starting")
    parser = argparse.ArgumentParser(description="Train gender model")
    parser.add_argument("--input", nargs='?', required=True, help="Gender embeddings json; file")
    parser.add_argument("--output", nargs='?', required=True, help="Model output file")

    args = parser.parse_args(args=argv)

    config = TrainConfig(
        test_size=0.05,
        random_state=42,
        n_trials=50,
    )

    model = train(args.input, config)

    model.save_model(args.output)
    logger.info(f"Model saved to {args.output}")


if __name__ == "__main__":
    main(sys.argv[1:])
