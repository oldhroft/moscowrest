import datetime
import hashlib
import json
import os

from requests import Session, Response
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from typing import Dict, Any, Union, Optional


def hash256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def response_meta(response: Response) -> Dict[str, Any]:

    meta = {
        "ok": response.ok,
        "elapsed": response.elapsed.total_seconds(),
        "status_code": response.status_code,
        "reason": response.reason,
        "url": response.url,
        "encoding": response.encoding,
        "headers": dict(response.headers),
        "dttm": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": hash256(response.text),
    }

    return meta


def dump_json(obj: Union[list, dict], filename: str) -> None:
    with open(filename, mode="w", encoding="utf-8") as file:
        json.dump(obj, file)


def load_json(filename: str) -> Union[list, dict]:
    with open(filename, mode="r", encoding="utf-8") as file:
        obj = json.load(file)
    return obj


def restore_from_cache(input: str, url: str) -> Optional[dict]:
    hash_url = f"chunk_{hash256(url)}.json"
    filename = os.path.join(input, hash_url)
    result = None
    if os.path.exists(filename):
        data = load_json(filename)
        if not isinstance(data, dict):
            raise TypeError("restore_from_cache supports only dict jsons")
        elif data["ok"]:
            result = data
        else:
            result = None
    else:
        result = None
    return result


def dump_to_cache(data: Union[dict, list], output: str, url: str) -> None:
    hash_url = f"chunk_{hash256(url)}.json"
    filename = os.path.join(output, hash_url)
    dump_json(data, filename)


def get_session(
    n_retries: int,
    backoff: int,
    status_forcelist: list=[500, 502, 503, 504],
    user_agent: Optional[str] = None,
) -> Session:
    session = Session()
    retries = Retry(
        total=n_retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    if user_agent is not None:
        session.headers.update({'User-Agent': user_agent})

    return session

def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    return f'{parsed.scheme}://{parsed.netloc}'
