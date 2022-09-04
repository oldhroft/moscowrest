from typing import TypedDict
from bs4 import BeautifulSoup
from bs4.element import Tag
from typing import Optional, List, cast


class ParsedItem(TypedDict):
    title: Optional[str]
    rating: Optional[int]
    cuisine: Optional[str]
    phone: Optional[str]
    link: Optional[str]
    city: Optional[str]
    address: Optional[str]


class ParsedData(ParsedItem):
    url: str
    dttm: str
    fname: str


def _extract_value(
    soup: BeautifulSoup, class_name, raise_error: bool = False
) -> Optional[str]:
    span = soup.find("span", class_=class_name)
    if isinstance(span, Tag):
        item = span.find("span", class_="value-title")
        if isinstance(item, Tag):
            return item.attrs["title"]
        elif raise_error:
            raise ValueError(f"Value-title not found")
        else:
            return None
    elif raise_error:
        raise ValueError(f"Span {class_name} not found, rebuild parsing!")
    else:
        return None


def parse_item(x: str) -> ParsedItem:

    soup = BeautifulSoup(x, "html.parser")

    fn_org = soup.find("span", "fn org")
    if isinstance(fn_org, Tag):
        title = fn_org.text
    else:
        raise ValueError("Title not found")

    link_item = soup.find("a", class_="permalink")
    if isinstance(link_item, Tag):
        link = link_item.attrs["href"]
    else:
        link_item = soup.find("a", class_="clearfix")
        if isinstance(link_item, Tag):
            link = link_item.attrs["href"]
        else:
            raise ValueError("Link not found")

    rating_item = soup.find("span", class_="col col_2")
    if isinstance(rating_item, Tag):
        rating_raw = rating_item.find_all("i", class_="i-star orange")
    else:
        rating_item = soup.find("span", class_="stars")
        if isinstance(rating_item, Tag):
            rating_raw = rating_item.find_all("i", class_="i-star orange")
        else:
            raise ValueError("Rating not found")
    rating = len(rating_raw)

    cuisine_item = soup.find("span", class_="col col_3")
    if isinstance(cuisine_item, Tag):
        cuisine = cuisine_item.text
    else:
        cuisine_item = soup.find("span", class_="cuisine")
        if isinstance(cuisine_item, Tag):
            cuisine = cuisine_item.text
        else:
            raise ValueError("Cuisine not found!")

    phone = _extract_value(soup, "tel", True)
    city = _extract_value(soup, "locality", True)
    address = _extract_value(soup, "street-address", True)

    return {
        "title": title,
        "rating": rating,
        "cuisine": cuisine,
        "phone": phone,
        "link": link,
        "city": city,
        "address": address,
    }


def parse_data(data: dict, fname: str) -> List[ParsedData]:
    result: List[ParsedData] = []
    if not data["ok"]:
        return result

    for card in data["data"]["cards"]:
        item = {"dttm": data["dttm"], "url": data["url"], "fname": fname}
        item.update(parse_item(card))
        # Danger zone, https://github.com/python/mypy/issues/11753
        item_casted = cast(ParsedData, item)
        result.append(item_casted)

    return result


import click
import glob
import os

from itertools import chain

from joblib import Parallel, delayed

from pandas import DataFrame

from src_rest.transformers.utils import load_process_json
from src_rest.loaders.utils import check_paths


@click.command()
@click.option("--input", help="input data folder", type=click.STRING, required=True)
@click.option(
    "--output", help="desitnation file to save data", required=True, type=click.STRING
)
@click.option(
    "--n_jobs",
    help="Number of jobs to perform the task",
    required=True,
    type=click.INT,
)
def process_mos_rest(input: str, output: str, n_jobs=-1) -> None:
    check_paths(input, output)
    files = glob.glob(os.path.join(input, "*.json"))
    result = map(delayed(lambda x: load_process_json(x, parse_data)), files)
    result = Parallel(n_jobs=n_jobs)(result)
    df = DataFrame(chain(*result), columns=list(ParsedData.__annotations__.keys()))
    df.to_csv(output, index=None)
