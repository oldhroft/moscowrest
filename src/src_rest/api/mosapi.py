import json

from typing import Union
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry

from src_rest.api.utils import get_query


class MosApi:
    """`Mos api instance"""

    def __init__(
        self, api_key: str, n_retries: int = 10, backoff: int = 1,
    ) -> None:
        api_string = f"&api_key={api_key}"
        object_string = "https://apidata.mos.ru/v1/{object_type}/{object_id}"
        base_query = "/rows?$skip={skip}&$top={top}"
        self.query = object_string + base_query + api_string
        self.count_query = object_string + "/count?" + api_string
        self.n_retries = n_retries
        self.backoff = backoff
        session = Session()
        retries = Retry(
            total=self.n_retries,
            backoff_factor=self.backoff,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session = session

    def get_response_text(self, response: Response) -> str:
        if response.ok:
            return response.text
        else:
            raise ValueError(
                f"Response not ok.\nText: {response.text}\nReason:{response.reason}"
            )

    def get(
        self, object_type: str, object_id: Union[str, int], skip: int, top: int
    ) -> Union[list, dict]:

        response = get_query(
            self.session,
            self.query,
            object_type=object_type,
            object_id=object_id,
            skip=skip,
            top=top,
        )
        text = self.get_response_text(response)
        return json.loads(text)

    def count(self, object_type: str, object_id: Union[str, int]) -> int:
        response = get_query(
            self.session,
            self.count_query,
            object_type=object_type,
            object_id=object_id,
        )

        text = self.get_response_text(response)
        return int(text)

import time

class MosDataset:

    def __init__(
        self, api: MosApi, dataset_id: Union[str, int], step: int = 1000,
        sleep: int=3,
    ) -> None:

        self.api = api
        self.step = step
        self.dataset_id = dataset_id
        self.object_type = "datasets"
        self.sleep = sleep

    def load(self):
        full_count = self.api.count(self.object_type, self.dataset_id)
        i = 0
        while True:
            time.sleep(self.sleep)
            print(f"Request {i}: {(i + 1) * self.step} / {full_count}")
            result = self.api.get(
                object_type=self.object_type,
                object_id=self.dataset_id,
                skip=i * self.step,
                top=self.step,
            )

            yield result

            if len(result) < self.step:
                break
            i += 1
            
