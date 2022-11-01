import os

import click
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.base import BaseEstimator
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import TruncatedSVD
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline

from pandas import read_csv

from joblib import dump

from src_rest.loaders.utils import check_paths


import logging

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger()


def create_model() -> BaseEstimator:
    model = make_pipeline(
        TfidfVectorizer(max_df=0.3, min_df=5),
        TruncatedSVD(n_components=300, random_state=17),
        StandardScaler(),
        LogisticRegression(random_state=17, n_jobs=-1, C=1, max_iter=500),
    )
    return model


@click.command()
@click.option("--input", help="Input data path", required=True, type=click.STRING)
@click.option("--output", help="Model artifact path", required=True, type=click.STRING)
def train_sentiment_model(input: str, output: str) -> None:
    check_paths(input, output, is_output_dir=True)
    data = read_csv(input)
    X = data["review"]
    y = data["sentiment"]
    logger.info("Creating model")
    model = create_model()
    logger.info("Fitting model")
    model.fit(X, y)
    logger.info("Dumping model")
    dump(model, os.path.join(output, "logreg_sentiment.sav"))
