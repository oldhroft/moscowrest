import glob
import json
import os

import click

from typing import Any, List

from pandas import DataFrame, concat, read_csv

from src_rest.loaders.utils import check_paths

import logging

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger()


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option("--is_list", help="is data in folder list, default = True", is_flag=True)
@click.option(
    "--output", help="desitnation file to save data", required=True, type=click.STRING
)
@click.option("--format", help="format to save data", default="json", type=click.STRING)
def concat_data(input: str, is_list: bool, output: str, format: str) -> None:
    """Concatenetes data jsons from input dir
    Parameters
    ----------
    input: str
        nput data folder
    is_list: bool
        is data in folder list
    format: str
        format to save data
    """

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
    boxcox_normalize,
)


def preprocess_df_text(df: DataFrame, col_text: str, n_jobs: int) -> DataFrame:
    """Preprocesses texts and adds columns with standardized, normalized texts
    and five sentiments
    
    Parameters
    ----------
    df: DataFrame
        DataFrame with raw texts
    col_text: str
        Column containing text
    n_jobs: str
        Number of jobs to used in normalization
    
    Returns
    -------
    df: DataFrame
        DataFrame with with features
        Contains column with raw text, standardized text, normalized text 
        and five sentiments
    
    Notes
    -----
    Sentiment is done using score_texts_dostoevsky function
    Normalization may take a while
    """
    texts = df[col_text]
    logger.info("Standartizing texts")
    texts_cleared = clear_texts(texts)
    logger.info("Normalizing texts")
    texts_n = preprocess_texts(texts_cleared.tolist(), n_jobs=n_jobs)
    logger.info("Scoring texts")
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
    """Extracts named aspects from texts
    Parameters
    ----------
    df: DataFrame
        DataFrame, that contains [col_text] column with texts,
        col_sentence column with sentence_id (cause aspects could be found at different places)
        col_id column and global_features columns
    col_text: str
        Column in input data containing texts (sentences)
    col_sentence: str
        Column containing sentence_id
    col_id: str
        Column containing restoraunt id
    global_features: List[str]
        Features to add to a resulting dataset
    
    Returns
    -------
    aspects_df: DataFrame
        DataFrame with parsed aspects (aspect_name, aspect_value)
    
    Notes
    -----
    The extracted properties are aspect_name and aspect_value
    e.g. aspect_name = "dish", aspect_value = "грузинская"
    
    """
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
@click.option("--input", help="input data path", type=click.STRING, required=True)
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
    """Creates different version (standardized, normalized of a text
    and calculates its sentiment
    
    Parameters
    ----------
    input: str
        Path to raw texts
    output: str
        Path to resulting dataset with features
        Containts column with raw text, standardized text, normalized text 
        and five sentiments
    col_text: str
        Column containing text
    n_jobs: str
        Number of jobs to used in normalization
    """
    check_paths(input, output)
    logger.info("Reading data")
    data = read_csv(input)
    logger.info("Start text preprocessing")
    data = preprocess_df_text(data, col_text=col_text, n_jobs=n_jobs)
    logger.info("Saving data")
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
    help="column containing restoraunt id",
    default="global_id",
    type=click.STRING,
)
@click.option(
    "--global_features",
    help="Extra features to add",
    default=["source", "url", "negative", "positive", "neutral", "skip", "speech"],
    multiple=True,
)
def create_named_aspects(
    input: str,
    output: str,
    col_text: str,
    col_sentence: str,
    col_id: str,
    global_features: List[str],
) -> None:
    """CLI to extract named aspects from texts
    Parameters
    ----------
    input: str
        Path to file with texts with sentences to search aspects
        Should contain col_text, col_sentence, col_id
    output: str
        File to save extracted named aspects
    col_text: str
        Column in input data containing texts (sentences)
    col_sentence: str
        Column containing sentence_id
    col_id: str
        Column containing restoraunt id
    global_features: List[str]
        Features to add to a resulting dataset

    Notes
    -----
    Named aspects are extracted using text parsing
    May take a while
    """
    check_paths(input, output)
    logger.info("Reading data")
    data = read_csv(input)
    logger.info("Creating aspects")
    df = find_aspects(data, col_text, col_sentence, col_id, global_features)
    logger.info("Saving data")
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
    """[TO REMOVE]
    Transformation of sentiments data from rureviews and senteval datasets
    See desc https://github.com/sismetanin/sentiment-analysis-in-russian
    """

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


def aggregate_named_aspects(df_aspects: DataFrame) -> DataFrame:
    """Create rating of named aspects
    Parameters
    ----------
    df_aspects: DataFrame
        Contains named aspects of restoraunts from different sources
    
    Returns
    -------
    df_agg: DataFrame
        DataFrame with aspects average sentiment (rating/score)
    
    Notes
    -----
    Takes parsed named aspects as an input, calculates their TOTAL sentiment out of neg, pos, neu
    using calculate_overall_sentiment() and then aggreates at restoraunt level
    """

    df_aspects = calculate_overall_sentiment(df_aspects)

    df_agg = df_aspects.groupby(["global_id", "aspect", "value"], as_index=False).agg(
        {"sentiment": "mean", "count": "sum"}
    )

    return df_agg


def aggregate_aspects(df_aspects: DataFrame) -> DataFrame:
    """Restoraunt-level aggregation of common aspects rating

    Parameters
    ----------
    df_aspects: DataFrame
        dataset with aspects from different sources labeled with fname col
        Must contain global_id, fname and rating columns

    Returns
    --------
    df_rating: DataFrame
        dataframe with columns global_id, rating
        rating column contains aggregated normalized ratings derived
        from common aspects from different sources

    Notes
    -----
    Aggregations are first done at source level, then normalized using box-xoc
    and then aggregation is done ate restoraunt level
    Final distributions are expected to have approx normal form

    """
    df_agg = df_aspects.groupby(["global_id", "fname"], as_index=False).rating.mean()

    df_agg["rating_boxcox_scaled"] = df_agg.groupby(["fname"]).rating.apply(
        boxcox_normalize
    )

    return df_agg.groupby(["global_id"], as_index=False).rating.mean()


@click.command()
@click.option(
    "--input", help="Input aspects data path", required=True, type=click.STRING
)
@click.option(
    "--output", help="Output aspects data path", required=True, type=click.STRING
)
def create_named_aspects_rating(input: str, output: str) -> None:
    """Creates rated named aspects list (for each rest)

    Parameters
    ----------
    input: str
        Path to directory with named aspects from different sources
    output: str
        File to save the resulting rating
    
    Notes
    -----
    Named aspects are rated based on sentiment of corr sentence
    Aggregation of sentiments is done in aggregate_named_aspects()
    """
    check_paths(input, output)
    paths = glob.glob(os.path.join(input, "*.csv"))
    logger.info("Reading data")
    data = concat(map(read_csv, paths), ignore_index=True)
    logger.info(f"Data shape, {data.shape}")
    logger.info("Aggregating aspects")
    data_agg = aggregate_named_aspects(data)
    logger.info(f"Aspects shape {data_agg.shape}, saving data")
    data_agg.to_csv(output, index=None)


@click.command()
@click.option(
    "--input", help="Input aspects data path", required=True, type=click.STRING
)
@click.option(
    "--output", help="Output aspects data path", required=True, type=click.STRING
)
def create_rating_from_aspects(input: str, output: str) -> None:
    """CLI to create restoraunt rating based on commin aspects score

    Parameters
    ----------
    input: str
        Path to directory with rated aspects
    output: str
        File to save resulting rating

    Notes
    -----
    Aspects are collected from different sources
    The function collects them, concats, normalizes and concatenates
    """
    check_paths(input, output)
    paths = glob.glob(os.path.join(input, "*.csv"))
    logger.info("Reading data")

    def _read_csv(fname):
        return read_csv(fname).assign(fname=fname)

    data = concat(map(_read_csv, paths), ignore_index=True)
    logger.info(f"Data shape, {data.shape}")
    logger.info("Aggregating aspects")
    data_agg = aggregate_aspects(data)
    logger.info(f"Aspects shape {data_agg.shape}, saving data")
    data_agg.to_csv(output, index=None)


def create_final_dataframe(
    df_md: DataFrame, df_mr: DataFrame, df_rating: DataFrame
) -> DataFrame:
    """Creates final dataframe for restoraunt ranking
    Parameters
    ----------
    df_md: DataFrame
        Restoraunt list with features from mosdata, used as standard restoraunt list
    df_mr: DataFrame
        Restoraunt list with features from moscow-restoraunts.ru
    df_rating: DataFrame
        Restoraunt rating. Created by aggregating common aspects scores
        by create_rating_from_aspects()

    Returns
    -------
    df: DataFrame
        Final dataframe with features and rating

    """

    # TODO: remove hardcode
    mr_columns = ["url", "cuisine", "phone", "avg_check", "global_id", "opening_hours"]

    md_columns = [
        "global_id",
        "Name",
        "Address",
        "SeatsCount",
        "SocialPrivileges",
        "x_coord",
        "y_coord",
    ]

    df = df_md[md_columns].merge(df_mr[mr_columns], how="inner", on="global_id")
    df = df.merge(df_rating, how="left", on="global_id")
    df.rating = df.rating.fillna(0)

    return df


@click.command()
@click.option(
    "--input", help="Path to global dataset", required=True, type=click.STRING
)
@click.option(
    "--input_mosrest",
    help="Path to moscow-restoraunts dataset",
    required=True,
    type=click.STRING,
)
@click.option("--input_rating", help="Path to rating", required=True, type=click.STRING)
@click.option(
    "--output", help="Output aspects data path", required=True, type=click.STRING
)
def create_final_dataset(
    input: str, input_mosrest: str, input_rating: str, output: str
) -> None:
    """CLI to create final dataset for restoraunt ranking
    Parameters
    ----------
    input: str
        Path to restoraunt list with features from mosdata,
        used as standard restoraunt list
    input_mosrest: str
        Path to restoraunt list with features from moscow-restoraunts.ru
    input_rating: str
        Path to restoraunt rating. Created by aggregating common aspects scores
        by create_rating_from_aspects()

    """
    check_paths(input, output)
    check_paths(input_rating, output)
    check_paths(input_mosrest, output)
    df_md = read_csv(input)
    df_mr = read_csv(input_mosrest)
    df_rating = read_csv(input_rating)
    df = create_final_dataframe(df_md, df_mr, df_rating)
    df.to_csv(output, index=False)
