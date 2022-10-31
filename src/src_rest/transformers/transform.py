import glob
import json
import os

import click

from typing import Any, List

from pandas import DataFrame, concat, read_csv

from src_rest.loaders.utils import check_paths


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option("--is_list", help="is data in folder list, default = True", is_flag=True)
@click.option(
    "--output", help="desitnation file to save data", required=True, type=click.STRING
)
@click.option("--format", help="format to save data", default="json", type=click.STRING)
def concat_data(input: str, is_list: bool, output: str, format: str) -> None:

    check_paths(input, output)

    data: List[Any] = []
    for filename in glob.glob(os.path.join(input, "*.json")):
        with open(filename, "r", encoding="utf-8") as file:
            chunk = json.load(file)

        if is_list:
            data.extend(chunk)
        else:
            data.append(chunk)

    if format == "json":
        with open(output, "w", encoding="utf-8") as file:
            json.dump(data, file)
    else:
        DataFrame(data).to_csv(output, index=None)


from src_rest.transformers.utils import (
    calculate_overall_sentiment,
    clear_texts,
    find_dish_aspects,
    preprocess_texts,
    score_texts_dostoevsky,
)


def preprocess_df_text(df: DataFrame, col_text: str, n_jobs: int) -> DataFrame:
    texts = df[col_text]
    texts_cleared = clear_texts(texts)
    texts_n = preprocess_texts(texts_cleared.tolist(), n_jobs=n_jobs)
    sentiment = DataFrame(score_texts_dostoevsky(texts_cleared))
    df = df.join(sentiment)
    return df.assign(
        **{f"{col_text}_norm": texts_n, f"{col_text}_standard": texts_cleared}
    )


def find_aspects(
    df: DataFrame,
    col_text: str,
    col_sentence: str = "sentence_id",
    col_id: str = "global_id",
    global_features: List[str] = ["source", "url"],
) -> DataFrame:
    texts = df[col_text].fillna("").str.split().tolist()
    ids = df[col_id].tolist()
    sentence_ids = df[col_sentence].tolist()
    df1 = find_dish_aspects(texts, ids, sentence_ids)
    aspects_df = concat([df1], axis=1, ignore_index=False)

    aspects_df = aspects_df.merge(
        df[[col_id, col_sentence, *global_features]],
        how="left",
        on=[col_id, col_sentence],
    )

    return aspects_df


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option(
    "--output", help="desitnation file to save data", required=True, type=click.STRING
)
@click.option(
    "--col_text", help="Column containing text", required=True, type=click.STRING
)
@click.option(
    "--n_jobs", help="Number of jobs to perform the task", default=-1, type=click.INT
)
def create_text_features(input: str, output: str, col_text: str, n_jobs: int) -> None:
    check_paths(input, output)
    data = read_csv(input)
    data = preprocess_df_text(data, col_text=col_text, n_jobs=n_jobs)
    data.to_csv(output, index=None)


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option(
    "--output", help="desitnation file to save data", required=True, type=click.STRING
)
@click.option(
    "--col_text", help="Column containing text", required=True, type=click.STRING
)
@click.option(
    "--col_sentence",
    help="column containing sentence id",
    default="sentence_id",
    type=click.STRING,
)
@click.option(
    "--col_id",
    help="column containing entity id",
    default="global_id",
    type=click.STRING,
)
@click.option(
    "--global_features",
    help="Extra features to add",
    default=["source", "url", "negative", "positive", "neutral", "skip", "speech"],
    multiple=True,
)
def create_aspects(
    input: str,
    output: str,
    col_text: str,
    col_sentence: str,
    col_id: str,
    global_features: List[str],
) -> None:
    check_paths(input, output)
    data = read_csv(input)
    df = find_aspects(data, col_text, col_sentence, col_id, global_features)
    df.to_csv(output, index=None)


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option(
    "--dataset", help="rureviews|senteval2016", type=click.STRING, required=True
)
@click.option(
    "--output", help="destination file to save data", required=True, type=click.STRING
)
def transform_sentiments(input: str, dataset: str, output: str):

    if dataset not in ("rureviews", "senteval2016"):
        raise ValueError("Unknown dataset!")
    check_paths(input, output)
    data = read_csv(input)
    if dataset == "rureviews":
        data.sentiment = data.sentiment.replace(
            {"negative": -1, "positive": 1, "neautral": 0}
        )
    else:
        data.sentiment = data.sentiment.replace({2: -1, 1: 1, 0: 0})

    data["review"] = clear_texts(data.review)
    (
        data.loc[data.review.notna() & (data.review.str.strip() != "")]
        .reset_index(drop=True)
        .to_csv(output, index=None)
    )


def aggregate_aspects(df_aspects: DataFrame) -> DataFrame:

    df_aspects = calculate_overall_sentiment(df_aspects)

    df_agg = df_aspects.groupby(["global_id", "aspect", "value"], as_index=False).agg(
        {"sentiment": "mean", "count": "sum"}
    )

    return df_agg


@click.command()
@click.option(
    "--input", help="Input aspects data path", required=True, type=click.STRING
)
@click.option(
    "--output", help="Output aspects data path", required=True, type=click.STRING
)
def collect_aspects(input, output) -> None:
    check_paths(input, output)
    paths = glob.glob(os.path.join(input, "*.csv"))
    data = concat(map(read_csv, paths), ignore_index=True)
    data_agg = aggregate_aspects(data)
    data_agg.to_csv(output, index=None)
