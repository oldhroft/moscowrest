import glob
import json
import os

import click

from typing import Any, List

@click.command()
@click.option('--input', help='input data folder', type=click.STRING, required=True)
@click.option('--is_list', help='is data in folder list, default = True', is_flag=True)
@click.option('--output', help='desitnation file to save data', required=True, type=click.STRING)
def concat_data(input: str, is_list: bool, output: str) -> None:

    if not os.path.exists(input):
        raise ValueError(f'Input path {os.path.abspath(input)} not found')

    data: List[Any] = []
    for filename in glob.glob(os.path.join(input, '*.json')):
        with open(filename, 'r', encoding='utf-8') as file:
            chunk = json.load(file)
        
        if is_list:
            data.extend(chunk)
        else:
            data.append(chunk)
            
    with open(output, 'w', encoding='utf-8') as file:
        json.dump(data, file)