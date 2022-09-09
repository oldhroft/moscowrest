import datetime
import hashlib
import json
import os
from bs4 import BeautifulSoup

from requests import Session, Response
from requests.adapters import HTTPAdapter, Retry

from urllib.parse import urlparse

from typing import Dict, Any, Union, Optional, Callable, Tuple

JSON_TYPE = Union[list, dict]


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


def load_json(filename: str) -> JSON_TYPE:
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


def dump_to_cache(data: JSON_TYPE, output: str, url: str) -> None:
    hash_url = f"chunk_{hash256(url)}.json"
    filename = os.path.join(output, hash_url)
    dump_json(data, filename)


def get_session(
    n_retries: int,
    backoff: int,
    status_forcelist: list = [500, 502, 503, 504],
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
        session.headers.update({"User-Agent": user_agent})

    return session


def get_base_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def scrape_page(
    session: Session,
    url: str,
    timeout: int,
    func: Callable[[BeautifulSoup], JSON_TYPE],
) -> JSON_TYPE:

    response = session.get(url, timeout=timeout)
    meta = response_meta(response)
    data = meta.copy()

    if not response.ok:
        data["data"] = {}
        return data
    try:
        info = func(BeautifulSoup(response.text, "html.parser"))
        data["data"] = info
        return data
    except Exception as e:
        raise ValueError(
            f"{func.__name__} failed on link {url} with exception:\n{str(e)}"
        )

def dump_scrape_page(
    session: Session,
    url: str,
    timeout: int,
    func: Callable[[BeautifulSoup], JSON_TYPE],
    output: str,
    cache: bool = False,
    return_data: bool = False,
    verbose: bool = True
) -> Optional[JSON_TYPE]:

    if cache:
        cached_data = restore_from_cache(output, url)

    if not cache or cached_data is None:
        if verbose:
            print(f"Parsing data from {url}")
        data = scrape_page(session, url, timeout, func)
        dump_to_cache(data, output, url)
        if return_data:
            return data
        else: return None
    else:
        if verbose:
            print(f"Restoring data from {url}")
        if return_data:
            return cached_data
        else:
            return None
        
