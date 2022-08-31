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
    limit: Optional[int]
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
        limit=limit
    )
    crawler.load_data()
