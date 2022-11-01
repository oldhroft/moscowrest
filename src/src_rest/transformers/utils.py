import re
from typing import Any, Dict, List


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


from typing import Union
from pandas import Series
from numpy import ndarray, radians, sin, cos, arcsin, sqrt

ARRAY_TYPE = Union[ndarray, Series]


def haversine_vectorize(
    lon1: ARRAY_TYPE, lat1: ARRAY_TYPE, lon2: ARRAY_TYPE, lat2: ARRAY_TYPE
):

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    newlon = lon2 - lon1
    newlat = lat2 - lat1

    haver_formula = (
        sin(newlat / 2.0) ** 2 + cos(lat1) * cos(lat2) * sin(newlon / 2.0) ** 2
    )

    dist = 2 * arcsin(sqrt(haver_formula))
    km = 6_367_000 * dist
    return km


from pymorphy2 import MorphAnalyzer


def normalize(sequence: list, morph: MorphAnalyzer) -> list:
    return list(map(lambda x: morph.parse(x)[0].normal_form, sequence))


from collections import Counter


def search_words(seq: List[str], words: Union[set, list]) -> Dict[str, int]:
    words = set(words)
    cnt: Dict[str, int] = Counter()
    for item in seq:
        if item in words:
            cnt[item] += 1
    return dict(cnt)


from joblib import Parallel, delayed

PATTERN = "!|\"|\\#|\\$|%|\\&|'|\\(|\\)|\\*|\\+|,|\\-|\\.|/|:|;|<|=|>|\\?|@|\\[|\\\\|\\]|\\^|_|`|\\{|\\||\\}|\\~|—|«|»|\\d+"


def clear_texts(texts: Series):
    return texts.str.lower().str.replace(PATTERN, "").str.split().apply(" ".join)


def preprocess_texts(texts: List[str], n_jobs: int = -1) -> List[str]:
    morph = MorphAnalyzer()
    pattern = re.compile(PATTERN)
    texts_spl = list(map(str.split, texts))
    tasks = map(delayed(lambda x: normalize(x, morph)), texts_spl)
    texts_n = Parallel(n_jobs=n_jobs)(tasks)
    return list(map(" ".join, texts_n))


from pandas import DataFrame

DISHES = [
    "пицца",
    "паста",
    "бургер",
    "шаурма",
    "пельмень",
    "шашлык",
    "вино",
    "пиво",
    "коктейль",
    "суши",
    "ролл",
    "фастфуд",
    "рыба",
    "морепродукт",
    "краб",
    "креветка",
    "лосось",
    "салат",
    "цезарь",
    "стейк",
    "курица",
    "котлета",
    "суп",
    "гаспаччо",
    "кебаб",
    "тикка",
    "масала",
    "хотдог",
    "гренка",
]


def find_dish_aspects(
    texts: List[List[str]], ids: list, sentence_ids: list
) -> DataFrame:

    morph = MorphAnalyzer()
    normalized_dishes = normalize(DISHES, morph)
    n2d = dict(zip(normalized_dishes, DISHES))

    items = map(lambda x: search_words(x, normalized_dishes), texts)

    aspects = []
    for global_id, sentence_id, item in zip(ids, sentence_ids, items):
        for aspect, cnt in item.items():
            aspects.append(
                {
                    "global_id": global_id,
                    "sentence_id": sentence_id,
                    "aspect": "dish",
                    "value": n2d[aspect],
                    "count": cnt,
                }
            )
    aspects_df = DataFrame(aspects)
    return aspects_df


def score_texts_dostoevsky(texts: List[str]) -> List[Dict[str, float]]:
    from dostoevsky.tokenization import RegexTokenizer
    from dostoevsky.models import FastTextSocialNetworkModel

    tokenizer = RegexTokenizer()
    model = FastTextSocialNetworkModel(tokenizer=tokenizer)

    return model.predict(texts)


from numpy import exp


def calculate_overall_sentiment(df: DataFrame) -> DataFrame:
    val = exp(df[["positive", "negative", "neutral", "skip", "speech"]].values)
    softmax = val / val.sum(axis=1).reshape(-1, 1)
    polar_scores = [1, -1, 0, 0, 0]
    score = (softmax * polar_scores).sum(axis=1)
    score = boxcox_normalize(score, add_one=True)
    return df.assign(sentiment=score)


from scipy.stats import boxcox
from sklearn.preprocessing import scale


def boxcox_normalize(
    data: Union[Series, ndarray], add_one: bool = False
) -> Union[Series, ndarray]:
    if add_one:
        bx, _ = boxcox(data + 1)
    else:
        bx, _ = boxcox(data)
    bx_scaled = scale(bx)

    if isinstance(data, Series):
        return Series(bx_scaled, data.index, name=data.name)
    else:
        return bx_scaled
