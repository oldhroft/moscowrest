import json
import click
from pandas import DataFrame, NA, read_csv, Series

from src_rest.loaders.utils import check_paths


def process_record(item: dict) -> dict:

    result = {"global_id": item["global_id"], "Number": item["Number"]}
    result.update(item["Cells"])
    result["x_coord"] = result["geoData"]["coordinates"][0]
    result["y_coord"] = result["geoData"]["coordinates"][1]
    result["PublicPhone"] = list(
        map(lambda x: list(x.values())[0], result["PublicPhone"])
    )
    del result["geoData"]
    return result


@click.command()
@click.option("--input", help="Input data path", type=click.STRING, required=True)
@click.option("--output", help="Output data path", type=click.STRING, required=True)
def process_mosdata(input: str, output: str) -> None:
    with open(input, "r", encoding="utf-8") as file:
        data = json.load(file)

    check_paths(input, output)

    data_expanded = DataFrame(list(map(process_record, data)))
    data_expanded = data_expanded.replace([None], NA)
    data_expanded.to_csv(output, index=None)


import re

from src_rest.transformers.utils import (
    extract_patterns,
    STREET_PATTERNS,
    HOUSE_PATTERNS,
    BUIDING_PATTERNS,
)

TYPOS = {
    'Eхtra Virgin': 'Extra virgin',
    'ExtraVirgin': 'Extra virgin'
}

def create_mosdata_datamart(df: DataFrame) -> DataFrame:
    df = df.copy()

    df["max_number"] = df.groupby("global_id").Number.transform("max")

    df = df.loc[
        (df.Number == df.max_number)
        & (df.TypeObject.isin(["кафе", "бар", "ресторан"]))
    ].reset_index(drop=True).drop('max_number', axis=1)

    df.PublicPhone = df.PublicPhone.replace(["[]", "['нет телефона']"], NA)

    items = df.Address.str.lower().str.split(",")

    # emtpy df -> something weird

    street_info = (
        items.apply(extract_patterns, patterns=STREET_PATTERNS)
        .apply(Series)
        .replace([None], NA)
    )

    house_info = (
        items.apply(extract_patterns, patterns=HOUSE_PATTERNS)
        .apply(Series)
        .replace([None], NA)
    )

    building_info = (
        items.apply(extract_patterns, patterns=BUIDING_PATTERNS)
        .apply(Series)
        .replace([None], NA)
    )

    df = (
        df.join(street_info.add_prefix("Street"))
        .join(house_info.add_prefix("House"))
        .join(building_info.add_prefix("Building"))
    )

    name_pattern = "(?:ресторан |кафе )?(.+)"

    df["Name_norm"] = (
        df.Name.str.lower()
        .str.extract(name_pattern, flags=re.DOTALL, expand=False)
        .str.strip('«»"')
        .replace(TYPOS)
    )

    df['OperatingCompany_count'] = df.groupby('OperatingCompany').Name.transform('count')
    df['Name_norm_count'] = df.groupby('Name_norm').Name_norm.transform('count')

    return df


@click.command()
@click.option("--input", help="Input data path", type=click.STRING, required=True)
@click.option("--output", help="Output data path", type=click.STRING, required=True)
def mosdata_datamart(input: str, output: str) -> None:

    check_paths(input, output)
    read_csv(input).pipe(create_mosdata_datamart).to_csv(output, index=None)