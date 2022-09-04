import re
from typing import Any, Dict


STREET_PATTERNS = {
    "(?:улица|(?<!\w)ул\.|(?<!\w)ул)\s+(.+)": "улица",
    "(.+)\s+(?:улица|(?<!\w)ул\.|(?<!\w)ул)": "улица",
    "(?:переулок|(?<!\w)пер\.|(?<!\w)пер)\s+(.+)": "переулок",
    "(.+)\s+(?:переулок|(?<!\w)пер\.|(?<!\w)пер)": "переулок",
    "(?:шоссе|ш\.)\s+(.+)": "шоссе",
    "(.+)\s+(?:шоссе|(?<!\w)ш\.)": "шоссе",
    "(?:аллея)\s+(.+)": "аллея",
    "(.+)\s+(?:аллея)": "аллея",
    "(?:проспект|пр\.)\s+(.+)": "проспект",
    "(.+)\s+(?:проспект|пр\.)": "проспект",
    "(?:площадь|пл\.)\s+(.+)": "площадь",
    "(.+)\s+(?:площадь|пл\.)": "площадь",
    "(?:бульвар)\s+(.+)": "бульвар",
    "(.+)\s+(?:бульвар)": "бульвар",
    "(?:проезд|пр\.)\s+(.+)": "проезд",
    "(.+)\s+(?:проезд|пр\.)": "проезд",
    "(?:тупик)\s+(.+)": "тупик",
    "(.+)\s+(?:тупик)": "тупик",
    "(?:набережная)\s+(.+)": "набережная",
    "(.+)\s+(?:набережная)": "набережная",
}

HOUSE_PATTERNS = {
    "(?:дом|(?<!\w)д\.|(?<!\w)д)\s+(.+)": "дом",
    "(?:вл\.|владение)\s+(.+)": "владение",
    "(?:участок)\s+(.+)": "участок",
}

BUIDING_PATTERNS = {
    "(?:корпус|корп\.|к\.|(?<!\w)к)\s+(.+)": "корпус",
    "(?:строение|(?<!\w)стр\.|(?<!\w)с\.|(?<!\w)стр|(?<!\w)с)\s+(.+)": "строение",
    "(?:здание)\s+(.+)": "здание",
}


def extract_patterns(items: list, patterns: Dict[str, str]) -> Dict[str, Any]:
    for item in items:
        for key, value in patterns.items():

            match = re.search(key, item, flags=re.DOTALL)
            if match is not None:
                return {"Type": value, "Name": match.group(1).strip()}
    return {"Type": None, "Name": None}


import os
from typing import Callable, Union
from src_rest.scrapying.utils import load_json


def load_process_json(
    path: str, func: Callable[[Union[dict, list], str], Union[dict, list]]
) -> Union[dict, list]:
    data = load_json(path)
    basename = os.path.basename(path)
    return func(data, basename)
