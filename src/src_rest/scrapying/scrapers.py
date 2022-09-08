from abc import abstractmethod, ABCMeta
from bs4 import BeautifulSoup

from typing import Dict, Optional, List, cast

from src_rest.scrapying.utils import (
    get_base_url,
    get_session,
    dump_scrape_page,
)


class BaseScraper(metaclass=ABCMeta):
    def __init__(
        self,
        output: str,
        user_agent: str = None,
        cache: bool = True,
        n_retries: int = 10,
        backoff: int = 1,
        timeout: int = 10,
        limit: Optional[int] = None,
    ) -> None:
        self.headers: Dict[str, str] = {}

        self.n_retries = n_retries
        self.backoff = backoff
        self.cache = cache
        self.output = output
        self.user_agent = user_agent
        self.timeout = timeout

        self.session = get_session(
            self.n_retries, self.backoff, user_agent=self.user_agent
        )
        self.limit = limit

    @abstractmethod
    def parse_data(self, soup: BeautifulSoup) -> dict:
        pass


class BaseCrawler(BaseScraper):
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
        super().__init__(output, user_agent, cache, n_retries, backoff, timeout, limit)
        self.base_url = get_base_url(link)
        self.link = link

    @abstractmethod
    def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
        pass

    def parse(self, soup: BeautifulSoup) -> dict:
        data = self.parse_data(soup)
        data["next_link"] = self.get_next_link(soup)
        return data

    def get_data(self) -> None:

        data = cast(
            dict,
            dump_scrape_page(
                self.session,
                self.link,
                self.timeout,
                self.parse,
                self.output,
                self.cache,
                return_data=True,
            ),
        )
        self.link = data["data"].get("next_link", None)

    def load_data(self) -> None:
        if self.link is None:
            return
        i = 0
        while True:
            print(f"Parsing data from {self.link}")
            self.get_data()
            i += 1
            if self.link is None or (self.limit is not None and self.limit == i):
                print("Terminating!")
                break


from urllib.parse import urljoin
from bs4.element import Tag


class MosRestCrawler(BaseCrawler):
    def parse_data(self, soup: BeautifulSoup) -> Dict[str, list]:
        rests_upper = soup.find("ul", class_="l-restaurants clearfix")
        rests_vertical = soup.find("ul", class_="l-restaurants-vertical clearfix")
        if isinstance(rests_upper, Tag):
            cards0 = list(rests_upper.find_all("span", class_="vcard"))
        else:
            cards0 = []
        if isinstance(rests_vertical, Tag):
            cards1 = list(rests_vertical.find_all("span", class_="vcard"))
        else:
            cards1 = []
        return {"cards": list(map(str, cards0 + cards1))}

    def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
        parent = soup.find("div", class_="restaurants_rating clearfix")
        if isinstance(parent, Tag):
            pagination_item = parent.find(
                "ul", class_="l-links clearfix", recursive=False
            )
            if isinstance(pagination_item, Tag):
                pagination = pagination_item.find_all("a")
            else:
                return None
        else:
            return None

        flag = False
        for path in pagination:
            url = urljoin(self.base_url, path.attrs["href"])
            if flag:
                return url

            if url.strip("/") == self.link.strip("/"):
                flag = True
        return None


from joblib import Parallel, delayed


class BaseLinkScraper(BaseScraper):
    def __init__(
        self,
        links: List[str],
        output: str,
        user_agent: str = None,
        cache: bool = True,
        n_retries: int = 10,
        backoff: int = 1,
        timeout: int = 10,
        limit: Optional[int] = None,
        n_jobs: int = -1,
        backend: str = "threading",
    ) -> None:
        super().__init__(output, user_agent, cache, n_retries, backoff, timeout, limit)
        self.links = links
        self.backend = backend
        if backend == "threading" and n_jobs == -1:
            self.n_jobs = 30
        else:
            self.n_jobs = n_jobs

        if limit is not None:
            self.links = self.links[:limit]

    def load_data(self) -> None:

        result = map(
            delayed(
                lambda x: dump_scrape_page(
                    self.session,
                    x,
                    self.timeout,
                    self.parse_data,
                    self.output,
                    self.cache,
                )
            ),
            self.links,
        )

        Parallel(n_jobs=self.n_jobs, backend=self.backend)(result)
