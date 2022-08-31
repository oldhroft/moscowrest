from abc import abstractmethod, ABCMeta
from bs4 import BeautifulSoup

from typing import Dict, Optional

from src_rest.scrapying.utils import (
    get_base_url,
    response_meta,
    restore_from_cache,
    dump_to_cache,
    get_session,
)


class BaseCrawler(metaclass=ABCMeta):
    def __init__(
        self,
        link: str,
        output: str,
        user_agent: str = None,
        cache: bool = True,
        n_retries: int = 10,
        backoff: int = 1,
        timeout: int = 10,
        limit: Optional[int] = None,
    ) -> None:
        self.link = link
        self.headers: Dict[str, str] = {}

        self.n_retries = n_retries
        self.backoff = backoff
        self.cache = cache
        self.output = output
        self.user_agent = user_agent
        self.timeout = timeout
        self.base_url = get_base_url(link)

        self.session = get_session(
            self.n_retries, self.backoff, user_agent=self.user_agent
        )
        self.limit = limit

    @abstractmethod
    def parse_data(self, soup: BeautifulSoup) -> dict:
        pass

    @abstractmethod
    def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
        pass

    def get_data(self) -> dict:
        response = self.session.get(self.link, timeout=self.timeout)
        body = response_meta(response)
        if response.ok:
            soup = BeautifulSoup(response.text,  "html.parser")
            try:
                data = self.parse_data(soup)
            except Exception as e:
                raise ValueError(
                    f".parse_data failed on link {self.link} with exception:\n{str(e)}"
                )
            try:
                next_link = self.get_next_link(soup)
            except Exception as e:
                raise ValueError(
                    f".get_next_link failed on link {self.link} with exception:\n{str(e)}"
                )
        else:
            data = {}
            next_link = None
        body["data"] = data
        body["next_link"] = next_link
        return body

    def load_data(self) -> None:

        if self.link is None:
            return
        i = 0
        while True:
            if self.cache:
                cached_data = restore_from_cache(self.output, self.link)

            if not self.cache or cached_data is None:
                data = self.get_data()
                dump_to_cache(data, self.output, self.link)
            else:
                data = cached_data

            self.link = data["next_link"]

            i += 1

            if self.link is None or (self.limit is not None and self.limit == i):
                break


from urllib.parse import urljoin


class MosRestCrawler(BaseCrawler):
    def parse_data(self, soup: BeautifulSoup) -> Dict[str, list]:
        rests_upper = soup.find("ul", class_="l-restaurants clearfix")
        rests_vertical = soup.find("ul", class_="l-restaurants-vertical clearfix")
        if rests_upper is not None:
            cards0 = rests_upper.find_all("span", class_="vcard")
        else:
            cards0 = []
        if rests_vertical is not None:
            cards1 = rests_vertical.find_all("span", class_="vcard")
        else:
            cards1 = []
        return {"cards": list(map(str, cards0 + cards1))}

    def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
        parent = soup.find("div", class_="restaurants_rating clearfix")
        pagination = parent.find(
            "ul", class_="l-links clearfix", recursive=False
        ).find_all("a")

        flag = False
        for path in pagination:
            url = urljoin(self.base_url, path.attrs["href"])
            if flag:
                return url

            if url.strip("/") == self.link.strip("/"):
                flag = True
        return None
