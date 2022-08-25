import json
import click
from pandas import DataFrame, NA

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
@click.option('--input', help='Input data path', type=click.STRING, required=True)
@click.option('--output', help='Output data path', type=click.STRING, required=True)
def process_mosdata(input: str, output: str) -> None:
    with open(input, "r", encoding="utf-8") as file:
        data = json.load(file)

    check_paths(input, output)

    data_expanded = DataFrame(list(map(process_record, data)))
    data_expanded = data_expanded.replace([None], NA)
    data_expanded.to_csv(output, index=None)
