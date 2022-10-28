import json
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


class ParsedDetails:
    url: str
    dttm: str
    fname: str
    x_coord: Optional[float]
    y_coord: Optional[float]
    avg_check: Optional[str]
    opening_hours: Optional[str]
    street_address: Optional[str]
    street_locality: Optional[str]
    aspect_stars: Optional[str]
    review: Optional[str]


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


def parse_details(data: dict, fname: str) -> List[ParsedDetails]:

    if not data["ok"]:
        print(f"Missed data {fname}, {data['url']}")
        result: List[ParsedDetails] = []
        return result

    aspect = data["data"]["aspect_stars"]
    if isinstance(aspect, dict):
        aspect_json = json.dumps(aspect)
    else:
        aspect_json = None

    item = {
        "url": data["url"],
        "dttm": data["dttm"],
        "fname": fname,
        "x_coord": data["data"]["x_coord"],
        "y_coord": data["data"]["y_coord"],
        "avg_check": data["data"]["avg_check"],
        "opening_hours": data["data"]["opening_hours"],
        "street_address": data["data"]["street_address"],
        "address_locality": data["data"]["address_locality"],
        "aspect_stars": aspect_json,
        "review": data["data"]["review"],
    }
    item_details = cast(ParsedDetails, item)
    return [item_details]


import click
import glob
import os

from itertools import chain

from joblib import Parallel, delayed
from urllib.parse import urljoin

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
    base_url = "https://www.moscow-restaurants.ru/"
    df["link"] = df.link.apply(lambda x: urljoin(base_url, x))
    df = df.drop_duplicates(subset=["link"]).reset_index(drop=True)
    df.to_csv(output, index=None)


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
def process_mos_rest_detailed(input: str, output: str, n_jobs=-1) -> None:
    check_paths(input, output)
    files = glob.glob(os.path.join(input, "*.json"))
    result = map(delayed(lambda x: load_process_json(x, parse_details)), files)
    result = Parallel(n_jobs=n_jobs)(result)
    df = DataFrame(chain(*result), columns=list(ParsedDetails.__annotations__.keys()))
    df.to_csv(output, index=None)


from src_rest.transformers.utils import haversine_vectorize

from numpy import arange
from pandas import read_csv


def create_mos_rest_datamart(
    df: DataFrame, details: DataFrame, df_comp: DataFrame, dist_threshold: float
):
    df = (
        df.drop(["url", "dttm", "fname"], axis=1)
        .merge(details, how="inner", left_on="link", right_on="url")
        .assign(
            left_id=lambda x: arange(len(x)),
            key=1,
            title_norm=lambda x: x.title.str.lower(),
        )
    )

    df_comp["key"] = 1

    col_comp = ["global_id", "key", "x_coord", "y_coord", "Name_norm"]
    col_df = ["left_id", "key", "x_coord", "y_coord", "title_norm"]
    print("Creating cartesian product")
    data_product = df_comp[col_comp].merge(df[col_df], on="key")
    print("Calculating distances")
    data_product["distance"] = haversine_vectorize(
        data_product.x_coord_x,
        data_product.y_coord_x,
        data_product.x_coord_y,
        data_product.y_coord_y,
    )
    print("Data product shape", data_product.shape[0])

    print("Finding rests in close proximity")
    close_proximity = data_product.loc[lambda x: x.distance <= dist_threshold]
    print("Close proximity number", close_proximity.shape[0])
    print("Calculating close names")
    match1 = close_proximity.apply(lambda x: x.Name_norm in x.title_norm, axis=1)
    match2 = close_proximity.apply(lambda x: x.title_norm in x.Name_norm, axis=1)
    mapping = (
        close_proximity.loc[match1 | match2]
        .sort_values(by="distance")
        .loc[:, ["global_id", "left_id"]]
        .drop_duplicates(subset=["left_id"])
    )
    print("Number of matched", mapping.shape[0])

    print("Finding relation between data and general data")
    df_with_id = df.drop("key", axis=1).merge(mapping, how="inner", on="left_id")

    final_columns = [
        "url",
        "dttm",
        "fname",
        "title",
        "rating",
        "cuisine",
        "phone",
        "city",
        "address",
        "x_coord",
        "y_coord",
        "avg_check",
        "opening_hours",
        "street_address",
        "street_locality",
        "global_id",
    ]

    print("Selecting main result")
    df_main = df_with_id[final_columns]

    print("Selecting aspects")
    aspects = (
        df_with_id.loc[df_with_id.aspect_stars.notna(), ["global_id", "aspect_stars", "url"]]
        .reset_index(drop=True)
        .assign(aspect_stars=lambda x: x.aspect_stars.apply(json.loads))
    )

    aspects = (
        aspects.explode("aspect_stars")
        .join(aspects.add_suffix("_full"))
        .assign(
            rating=lambda x: x.apply(
                lambda y: y.aspect_stars_full[y.aspect_stars], axis=1
            )
        )
        .drop(["aspect_stars_full", "global_id_full"], axis=1)
    )

    print("Selecting reviews")
    reviews = df_with_id.loc[df_with_id.review.notna(), ["global_id", "review", "url"]].assign(
        source="https://www.moscow-restaurants.ru/"
    )

    print("Transforming reviews into sentences")

    reviews_sentence = (
        reviews.assign(review=lambda x: x.review.str.split('.'))
        .explode("review")
        .reset_index(drop=True)
        .loc[lambda x: x.review.str.strip() != ""]
    )

    reviews_sentence["sentence_id"] = arange(len(reviews_sentence))

    return df_main, aspects, reviews_sentence


@click.command()
@click.option("--input", help="Input data path", type=click.STRING, required=True)
@click.option(
    "--input_details",
    help="Input data path for details",
    type=click.STRING,
    required=True,
)
@click.option(
    "--input_global",
    help="Input data path for global data",
    type=click.STRING,
    required=True,
)
@click.option("--output", help="Output data path", type=click.STRING, required=True)
@click.option(
    "--output_aspect",
    help="Output data path for aspects",
    type=click.STRING,
    required=True,
)
@click.option(
    "--output_review",
    help="Output data path for reviews",
    type=click.STRING,
    required=True,
)
@click.option(
    "--dist_threshold",
    help="Distance threshold in meters",
    type=click.FLOAT,
    required=False,
    default=300,
)
def mos_rest_datamart(
    input: str,
    input_details: str,
    input_global: str,
    output: str,
    output_aspect: str,
    output_review: str,
    dist_threshold: float,
) -> None:

    check_paths(input, output)
    check_paths(input_details, output_aspect)
    check_paths(input_global, output_review)
    df = read_csv(input)
    details = read_csv(input_details)
    df_comp = read_csv(input_global)

    df_main, aspects, reviews = create_mos_rest_datamart(
        df, details, df_comp, dist_threshold=dist_threshold
    )

    df_main.to_csv(output, index=None)
    aspects.to_csv(output_aspect, index=None)
    reviews.to_csv(output_review, index=None)
