from typing import Type
from matplotlib.font_manager import json_dump, json_load
import pytest
import requests_mock
import requests
import datetime
import re

from src_rest.scrapying.utils import *
from src_rest.loaders.utils import safe_mkdir


class TestUtils:
    @pytest.fixture(autouse=True)
    def init_data(self):
        safe_mkdir("./test_sc_utils")

        yield

        os.system("rm -rf ./test_sc_utils")

    def test_hash256(self):

        url = "https://www.website.org"
        another_url = "https://www.another.website.org"

        hash_url = hash256(url)
        hash_another_url = hash256(another_url)
        hash_url_one_more_time = hash256(url)

        assert isinstance(hash_url, str)
        assert hash_url == hash_url_one_more_time
        assert hash_url != hash_another_url

    def test_response_meta(self):

        with requests_mock.Mocker() as m:
            m.get(
                "https://mocker-website.org/mock",
                text="Some text",
                status_code=200,
            )

            response = requests.get("https://mocker-website.org/mock")
            meta = response_meta(response)
            assert meta["ok"]
            text_hash = hash256("Some text")
            assert meta["sha256"] == text_hash

            for key in [
                "ok",
                "elapsed",
                "status_code",
                "reason",
                "url",
                "encoding",
                "dttm",
                "sha256",
                "headers",
            ]:
                assert key in meta

            assert meta["encoding"] == "utf-8"
            assert meta["dttm"] <= datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            assert (
                re.match("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$", meta["dttm"])
                is not None
            )
            assert meta["elapsed"] >= 0

    def test_json_dump_load(self):
        test_json = {"key": 1}
        json_dump(test_json, "./test_sc_utils/test_dump.json")

        assert os.path.exists("./test_sc_utils/test_dump.json")

        loaded_json = json_load("./test_sc_utils/test_dump.json")

        for key, item in test_json.items():
            assert item == loaded_json[key]

    def test_requests_meta_dump(self):
        with requests_mock.Mocker() as m:
            m.get(
                "https://mocker-website.org/mock",
                text="Some text",
                status_code=200,
            )
            response = requests.get("https://mocker-website.org/mock")
            meta = response_meta(response)

            json_dump(meta, "./test_sc_utils/meta_dump.json")

            assert os.path.exists("./test_sc_utils/meta_dump.json")

            meta_loaded = json_load("./test_sc_utils/meta_dump.json")

            for key, item in meta.items():
                assert item == meta_loaded[key]

    def test_get_session(self):
        session = get_session(1, 1)
        with requests_mock.Mocker() as m:
            m.get(
                "https://mocker-website.org/mock",
                text="Some text",
                status_code=200,
            )
            response = session.get(
                "https://mocker-website.org/mock",
            )
            assert response.text == "Some text"
            assert response.ok
            assert response.status_code == 200

        session = get_session(2, 0.01, user_agent="Chrome")

        assert session.headers["User-Agent"] == "Chrome"

        with requests_mock.Mocker() as m:
            m.get(
                "https://mocker-website.org/mock",
                text="Some text",
                status_code=500,
            )
            response = session.get("https://mocker-website.org/mock")
            assert response.text == "Some text"
            assert not response.ok
            assert response.status_code == 500

        response = session.get("http://proxyjudge.us/")
        assert "Chrome" in response.text

    def test_dump_to_cache_and_restore(self):

        data = {"key": 0, "key1": "value", "ok": True}

        url = "https://some.website.org"
        safe_mkdir("./test_sc_utils/dump")
        dump_to_cache(data, "./test_sc_utils/dump", url)

        assert len(os.listdir("./test_sc_utils/dump")) == 1

        data = restore_from_cache("./test_sc_utils/dump", url)
        assert data["key"] == 0
        assert data["key1"] == "value"

        data = {"key": 0, "key1": "value", "ok": False}
        url = "https://some.website.org"
        safe_mkdir("./test_sc_utils/dump")
        dump_to_cache(data, "./test_sc_utils/dump", url)

        assert len(os.listdir("./test_sc_utils/dump")) == 1

        data = restore_from_cache("./test_sc_utils/dump", url)
        assert data is None

    def test_restore_errors(self):
        url = "https://some.website.org"
        data1 = restore_from_cache("./test_sc_utils/dump1", url)
        assert data1 is None

        data = ["some", "data"]
        safe_mkdir("./test_sc_utils/dump1")

        dump_to_cache(data, "./test_sc_utils/dump1", url)

        assert len(os.listdir("./test_sc_utils/dump1")) == 1

        with pytest.raises(TypeError):
            restore_from_cache("./test_sc_utils/dump1", url)

    def test_base_url(self):
        url = "https://somewebsite.org/page1/page2"
        base_url = get_base_url(url)
        assert base_url == "https://somewebsite.org"


from src_rest.scrapying.scrapers import *


class TestScrapers:
    @pytest.fixture(autouse=True)
    def init_data(self):
        safe_mkdir("./scrapers_tmp")

        yield

        os.system("rm -rf ./scrapers_tmp")

    def test_base_crawler_creation(self):
        with pytest.raises(TypeError):
            BaseCrawler("https://some_website.org", output="./scrapers_tmp")

        class SimpleCrawler(BaseCrawler):
            def parse_data(self, soup: BeautifulSoup) -> dict:
                return {"key": "value"}

            def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
                return None

        crawler = SimpleCrawler("some_link", "some_path", user_agent="Chrome")
        assert isinstance(crawler, SimpleCrawler)
        assert issubclass(SimpleCrawler, BaseCrawler)
        assert isinstance(crawler, BaseCrawler)

        assert isinstance(crawler.session, requests.Session)
        assert crawler.session.headers["User-Agent"] == "Chrome"

    def test_base_crawler_parsing(self):

        sample_html = """<!DOCTYPE html>
        <html>
        <body>
        <h1>My First Heading</h1>
        <p>My first paragraph.</p>
        </body>
        </html>"""

        class SimpleCrawler(BaseCrawler):
            def parse_data(self, soup: BeautifulSoup) -> dict:

                heading = soup.find("h1")
                paragraph = soup.find("p")

                return {"heading": heading.text, "paragraph": paragraph.text}

            def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
                return None

        with requests_mock.Mocker() as m:
            url = "https://www.sample-html-website.org"
            m.get(url, text=sample_html, status_code=200)
            safe_mkdir("./scrapers_tmp/simple")

            crawler = SimpleCrawler(url, output="./scrapers_tmp/simple")

            data = crawler.get_data()

            for key in [
                "ok",
                "elapsed",
                "status_code",
                "reason",
                "url",
                "encoding",
                "dttm",
                "sha256",
                "headers",
                "data",
            ]:
                assert key in data

            assert data["sha256"] == hash256(sample_html)
            assert data["ok"]
            assert data["status_code"] == 200

            assert data["next_link"] is None

            assert data["data"]["heading"] == "My First Heading"
            assert data["data"]["paragraph"] == "My first paragraph."

            crawler.load_data()

            assert len(os.listdir("./scrapers_tmp/simple")) == 1
            hash_url = f"./scrapers_tmp/simple/chunk_{hash256(url)}.json"

            data = load_json(hash_url)

            assert data["sha256"] == hash256(sample_html)
            assert data["ok"]
            assert data["status_code"] == 200
            assert data["next_link"] is None
            assert data["data"]["heading"] == "My First Heading"
            assert data["data"]["paragraph"] == "My first paragraph."

            crawler.load_data()
            crawler.load_data()

            crawler = SimpleCrawler(url, output="./scrapers_tmp/simple")
            crawler.load_data()

            assert m.call_count == 2

    def test_base_crawling_parser_next_link(self):
        sample_html1 = """<!DOCTYPE html>
        <html>
        <body>
        <h1>My First Heading</h1>
        <p>My first paragraph.</p>
        </body>
        </html>"""

        sample_html2 = """<!DOCTYPE html>
        <html>
        <body>
        <h1>My second Heading</h1>
        <p>My second paragraph.</p>
        </body>
        </html>"""

        url1 = "https://www.website.org/page1"
        url2 = "https://www.website.org/page2"
        loc = "./scrapers_tmp/simple1"

        safe_mkdir(loc)

        class SimpleCrawler(BaseCrawler):
            def parse_data(self, soup: BeautifulSoup) -> dict:

                heading = soup.find("h1")
                paragraph = soup.find("p")

                return {"heading": heading.text, "paragraph": paragraph.text}

            def get_next_link(self, soup: BeautifulSoup) -> Optional[str]:
                if self.link == url1:
                    return url2
                else:
                    return None

        crawler = SimpleCrawler(url1, loc)
        with requests_mock.Mocker() as m:
            m.get(url1, text=sample_html1)
            m.get(url2, text=sample_html2)
            crawler.load_data()
            assert len(os.listdir(loc)) == 2

            restored = restore_from_cache(loc, url2)
            assert restored["data"]["heading"] == "My second Heading"


from bs4 import BeautifulSoup


class TestMosRestCrawler:
    @pytest.fixture(autouse=True)
    def init_data(self):
        safe_mkdir("./mos_rest")
        yield
        os.system("rm -rf ./mos_rest")

    def test_creation(self):
        crawler = MosRestCrawler(
            "https://website.org", output="./mos_rest", user_agent="Chrome"
        )
        assert isinstance(crawler, MosRestCrawler)
        assert issubclass(MosRestCrawler, BaseCrawler)
        assert isinstance(crawler, BaseCrawler)
        assert isinstance(crawler.session, requests.Session)
        assert crawler.session.headers["User-Agent"] == "Chrome"

    def test_parse_data(self):
        crawler = MosRestCrawler(
            "https://website.org/restaurants/", output="./mos_rest", user_agent="Chrome"
        )

        with open(
            "./src/src_rest/tests/data/index.html", "r", encoding="utf-8"
        ) as file:
            html = file.read()

        soup = BeautifulSoup(html)

        data = crawler.parse_data(soup)

        assert isinstance(data, dict)
        assert "cards" in data

        next_page = crawler.get_next_link(soup)
        assert next_page == "https://website.org/restaurants/?curPos=7"

        crawler = MosRestCrawler(
            "https://website.org/restaurants", output="./mos_rest", user_agent="Chrome"
        )

        next_page = crawler.get_next_link(soup)
        assert next_page == "https://website.org/restaurants/?curPos=7"
