import glob
import json
import os

import click

from typing import Any, List

from pandas import DataFrame

from src_rest.loaders.utils import check_paths

@click.command()
@click.option('--input', help='input data folder', type=click.STRING, required=True)
@click.option('--is_list', help='is data in folder list, default = True', is_flag=True)
@click.option('--output', help='desitnation file to save data', required=True, type=click.STRING)
@click.option('--format', help='format to save data', default='json', type=click.STRING)
def concat_data(input: str, is_list: bool, output: str, format: str) -> None:

    check_paths(input, output)

    data: List[Any] = []
    for filename in glob.glob(os.path.join(input, '*.json')):
        with open(filename, 'r', encoding='utf-8') as file:
            chunk = json.load(file)
        
        if is_list:
            data.extend(chunk)
        else:
            data.append(chunk)
    
    if format == "json":
        with open(output, 'w', encoding='utf-8') as file:
            json.dump(data, file)
    else:
        DataFrame(data).to_csv(output, index=None)
