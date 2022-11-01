import click
from typing import Optional

from src_rest.scrapying.scrapers import MosRestCrawler
from src_rest.loaders.utils import check_paths


@click.command()
@click.option(
    "--output", help="Output path to load data", type=click.STRING, required=True
)
@click.option(
    "--user_agent", default="Chrome", help="default user-agent", type=click.STRING
)
@click.option(
    "--cache",
    help="If caching of requests should be performed",
    is_flag=True,
)
@click.option(
    "--n_retries",
    default=10,
    help="number of retries when making requests",
    type=click.INT,
)
@click.option(
    "--backoff", default=0.1, help="backoff when making request", type=click.FLOAT
)
@click.option("--timeout", default=10, help="Timeout for request", type=click.INT)
@click.option("--limit", default=None, help="Timeout for request", type=click.INT)
def load_moscow_restaurants(
    output: str,
    user_agent: str,
    cache: bool,
    n_retries: int,
    backoff: int,
    timeout: int,
    limit: Optional[int],
) -> None:

    check_paths(input=None, output=output, is_output_dir=True)

    crawler = MosRestCrawler(
        link="https://www.moscow-restaurants.ru/restaurants/",
        output=output,
        user_agent=user_agent,
        cache=cache,
        n_retries=n_retries,
        backoff=backoff,
        timeout=timeout,
        limit=limit,
    )
    crawler.load_data()


from pandas import read_csv

from src_rest.scrapying.scrapers import MosRestScraper


@click.command()
@click.option("--input", help="Input data with links", type=click.STRING, required=True)
@click.option(
    "--output", help="Output path to load data", type=click.STRING, required=True
)
@click.option(
    "--user_agent", default="Chrome", help="default user-agent", type=click.STRING
)
@click.option(
    "--cache",
    help="If caching of requests should be performed",
    is_flag=True,
)
@click.option(
    "--n_retries",
    default=10,
    help="number of retries when making requests",
    type=click.INT,
)
@click.option(
    "--backoff", default=0.1, help="backoff when making request", type=click.FLOAT
)
@click.option("--timeout", default=10, help="Timeout for request", type=click.INT)
@click.option("--limit", default=None, help="Timeout for request", type=click.INT)
@click.option(
    "--backend", default="threading", help="parallel backend", type=click.STRING
)
@click.option("--n_jobs", default=-1, help="number of jobs", type=click.INT)
def load_moscow_restaurants_detailed(
    input: str,
    output: str,
    user_agent: str,
    cache: bool,
    n_retries: int,
    backoff: int,
    timeout: int,
    limit: Optional[int],
    backend: str,
    n_jobs: int,
) -> None:

    check_paths(input=input, output=output, is_output_dir=True)

    data = read_csv(input)
    links = data.link.values.tolist()

    crawler = MosRestScraper(
        links=links,
        output=output,
        user_agent=user_agent,
        cache=cache,
        n_retries=n_retries,
        backoff=backoff,
        timeout=timeout,
        limit=limit,
        n_jobs=n_jobs,
        backend=backend,
    )
    crawler.load_data()
