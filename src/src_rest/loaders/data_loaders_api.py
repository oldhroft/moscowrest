import click
import json
import os

from src_rest.api.mosapi import MosApi, MosDataset
from src_rest.loaders.utils import safe_mkdir


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
def load_mosdata(
    dataset_id: str,
    output: str,
    api_key: str,
    n_retries: int,
    backoff: int,
    sleep: int,
    step: int,
) -> None:
    api = MosApi(api_key, n_retries=n_retries, backoff=backoff)
    ds = MosDataset(api, dataset_id, sleep=sleep, step=step)
    safe_mkdir(output)
    for i, chunk in enumerate(ds.load()):
        print(f"Processing chunk {i}")
        chunk_name = os.path.join(output, f"chunk_{i}.json")
        with open(chunk_name, "w", encoding="utf-8") as file:
            json.dump(chunk, file)
