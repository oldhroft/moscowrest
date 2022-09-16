import click
import json
import os

from typing import Union

from src_rest.api.mosapi import MosApi, MosDataset
from src_rest.loaders.utils import check_paths


@click.command()
@click.option("--dataset_id", help="Dataset id to load", type=click.STRING, required=1)
@click.option(
    "--output", help="Output path to load data", type=click.STRING, required=1
)
@click.option("--api_key", help="your api key", type=click.STRING, required=1)
@click.option(
    "--n_retries",
    default=10,
    help="number of retries when making requests",
    type=click.INT,
)
@click.option(
    "--backoff", default=0.1, help="backoff when making request", type=click.FLOAT
)
@click.option("--sleep", default=3, help="Sleep between attempts", type=click.INT)
@click.option("--step", default=1000, help="Step size", type=click.INT)
@click.option("--limit", default=None, help="limit requests number", type=click.INT)
def load_mosdata(
    dataset_id: str,
    output: str,
    api_key: str,
    n_retries: int,
    backoff: int,
    sleep: int,
    step: int,
    limit: Union[int, None],
) -> None:
    api = MosApi(api_key, n_retries=n_retries, backoff=backoff)
    ds = MosDataset(api, dataset_id, sleep=sleep, step=step)
    check_paths(input=None, output=output, is_output_dir=True)
    for i, chunk in enumerate(ds.load()):
        print(f"Processing chunk {i}")
        chunk_name = os.path.join(output, f"chunk_{i}.json")
        with open(chunk_name, "w", encoding="utf-8") as file:
            json.dump(chunk, file)

        if limit is not None and i + 1 == limit:
            break


from pandas import read_csv


@click.command()
@click.option(
    "--output", help="Output path to load data", type=click.STRING, required=1
)
def load_sentiment(output) -> None:
    check_paths(input=None, output=output)
    url = "https://raw.githubusercontent.com/sismetanin/rureviews/master/women-clothing-accessories.3-class.balanced.csv"
    data = read_csv(url, delimiter="\t")
    data.to_csv(output)
