import pytest

from requests import Session
from requests import ConnectionError
import requests

from src_rest.api.utils import get_query

import json


def test_get_query():
    session = Session()
    query = "https://api64.ipify.org?format={format}"
    result = get_query(session, query, format="json")
    assert "ip" in json.loads(result.text)

    query = "https://api64.ipify.org/bundle/mundle"
    result = get_query(session, query)
    assert result.status_code == 404

    with pytest.raises(ConnectionError):
        query = "https://djdjdjdjdjdjdd124455.com"
        result = get_query(session, query)


import requests_mock
import json
from src_rest.api.mosapi import MosApi

TEST_CONF_API = {
    "n_retries": 1,
}


class TestMosApi:
    @pytest.fixture(autouse=True)
    def init_data(self):

        self.data = ["a", "b", "c", "d", "e", "f", "g"]

    def test_api_unauthorized(self):

        with requests_mock.Mocker() as m:
            m.get(
                "https://apidata.mos.ru/v1/object/96/rows?$skip=30&$top=40",
                status_code=403,
                text="testMocking1",
            )

            data = requests.get(
                "https://apidata.mos.ru/v1/object/96/rows?$skip=30&$top=40"
            )
            assert data.status_code == 403, "Mocker failed"
            assert data.text == "testMocking1", "Mocker failed, wrong text"

            api = MosApi("1223", **TEST_CONF_API)
            with pytest.raises(ValueError):
                api.get("object", "96", 30, 40)
            try:
                api.get("object", "96", 30, 40)
            except ValueError as e:
                assert "testMocking1" in str(e)
        api = MosApi("1233", **TEST_CONF_API)
        with pytest.raises(ValueError):
            api.get("object", "96", 30, 40)

    def test_api_get_data(self):
        api = MosApi("123", **TEST_CONF_API)
        with requests_mock.Mocker() as m:
            m.get(
                "https://apidata.mos.ru/v1/object/96/rows?$skip=3&$top=4&api_key=123",
                text=json.dumps(self.data[3:7]),
            )
            data = api.get("object", "96", 3, 4)

            assert len(data) == 4
            assert data[0] == "d"


from typing import Union, Dict, Any
from src_rest.api.mosapi import MosDataset

TEST_CONF_DATASET = {"sleep": 0}


class MosApiMocker:
    def __init__(
        self,
        api_key: str,
        n_retries: int = 10,
        backoff: int = 1,
    ) -> None:
        self.data: Dict[Any, Any] = {}
        self.data["0"] = list(range(10000))
        self.data["1"] = ["A", "B"]

    def get(
        self, object_type: str, object_id: Union[str, int], skip: int, top: int
    ) -> Union[list, dict]:
        if object_id not in self.data:
            raise ValueError("Text: Page not found")
        return self.data[object_id][skip : skip + top]

    def count(self, object_type: str, object_id: Union[str, int]) -> int:
        if object_id not in self.data:
            raise ValueError("Text: Page not found")
        return len(self.data[object_id])


class TestMosDataset:
    @pytest.fixture(autouse=True)
    def init_data(self):
        self.api = MosApiMocker("123")

    def test_no_object(self):
        ds = MosDataset(self.api, "34", **TEST_CONF_DATASET)
        items = []
        with pytest.raises(ValueError):
            for item in ds.load():
                items.extend(item)

    def test_load_data(self):
        ds = MosDataset(self.api, "0", **TEST_CONF_DATASET)
        data = []
        for item in ds.load():
            data.extend(item)

        assert len(data) == len(self.api.data["0"])
        assert str(data) == str(self.api.data["0"])

        ds = MosDataset(self.api, "1", **TEST_CONF_DATASET)
        data = []
        for item in ds.load():
            data.extend(item)

        assert len(data) == len(self.api.data["1"])
        assert str(data) == str(self.api.data["1"])
