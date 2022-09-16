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


import re


class MosRestScraper(BaseLinkScraper):
    def parse_data(self, soup: BeautifulSoup) -> dict:

        pattern = "Placemark\\(\\[(\\d+\\.\\d+),\\ (\\d+.\\d+)\\]"
        div_ymap = soup.find("div", class_="col col_img")
        if isinstance(div_ymap, Tag):
            script = div_ymap.find("script", type="text/javascript")
            if isinstance(script, Tag):

                match = re.search(pattern, script.text)
                if match is not None:
                    x = float(match.group(2))
                    y = float(match.group(1))
                else:
                    x = None
                    y = None
            else:
                raise ValueError("Maps script not found")
        else:
            x = None
            y = None

        main_data = soup.find("div", class_="data")

        if isinstance(main_data, Tag):
            avg_check_card = main_data.find("div", class_="row average_check")
            if isinstance(avg_check_card, Tag):
                avg_check = avg_check_card.text
            else:
                avg_check = None

            oh_card = main_data.find("meta", itemprop="openingHours")
            if isinstance(oh_card, Tag):
                whours = oh_card.attrs["content"]
            else:
                whours = None

            sa_card = main_data.find("meta", itemprop="streetAddress")
            if isinstance(sa_card, Tag):
                street_address = sa_card.attrs["content"]
            else:
                street_address = None

            al_card = main_data.find("meta", itemprop="addressLocality")
            if isinstance(al_card, Tag):
                address_locality = al_card.attrs["content"]
            else:
                address_locality = None

        else:
            raise ValueError("div with data not found")

        rest_stars = soup.find("div", class_="rest_stats")

        if not isinstance(rest_stars, Tag):
            aspect_stars = None
        else:
            titles = rest_stars.find_all("div", class_="title")
            stars = rest_stars.find_all("div", class_="stars")

            aspect_stars = {}
            for title, stars_div in zip(titles, stars):

                aspect_stars[title.text] = len(
                    stars_div.find_all("i", class_="i-star orange")
                )

        review = soup.find("div", class_="item-review-col_right")
        if not isinstance(review, Tag):
            text = None
        else:
            text_items = review.find_all("div", class_="data-text")
            text = '\n'.join(map(lambda x: x.text.strip("\r\n\t "), text_items))

        return {
            "x_coord": x,
            "y_coord": y,
            "avg_check": avg_check,
            "opening_hours": whours,
            "street_address": street_address,
            "address_locality": address_locality,
            "aspect_stars": aspect_stars,
            "review": text,
        }
