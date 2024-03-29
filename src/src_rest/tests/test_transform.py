import pytest
import json
import os

from click.testing import CliRunner

from src_rest.loaders.utils import safe_mkdir
from src_rest.transformers.transform import concat_data, preprocess_df_text


class TestConcatData:
    @pytest.fixture(autouse=True)
    def init_data(self):
        self.data1 = {"data": [1, 2, 3]}
        self.path1 = "./test_data/data1"

        safe_mkdir("./test_data/")
        safe_mkdir(self.path1)

        for i in range(10):
            with open(f"./test_data/data1/file{i}.json", "w", encoding="utf-8") as file:
                json.dump(self.data1, file)

        self.data2 = [1, 2, 3]
        self.path2 = "./test_data/data2"

        safe_mkdir("./test_data/")
        safe_mkdir(self.path2)

        for i in range(10):
            with open(f"./test_data/data2/file{i}.json", "w", encoding="utf-8") as file:
                json.dump(self.data2, file)

        safe_mkdir("./test_data/data3")

        yield

        os.system("rm -rf ./test_data")

    def test_dict_data(self):

        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            ["--input", "./test_data/data1", "--output", "./test_data/data1.json"],
        )

        assert result.exit_code == 0

        with open("./test_data/data1.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        assert len(data) == 10
        assert str(data[5]) == str(self.data1)

    def test_list_data(self):

        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            [
                "--input",
                "./test_data/data2",
                "--output",
                "./test_data/data2.json",
                "--is_list",
            ],
        )

        assert result.exit_code == 0

        with open("./test_data/data2.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        assert len(data) == 10 * 3
        assert data[3] == 1

    def test_unexisting_path(self):
        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            [
                "--input",
                "/path/that/doesnt/even/exist",
                "--output",
                "./somefile.json",
                "--is_list",
            ],
        )

        assert isinstance(result.exception, FileNotFoundError)

        assert "/path/that/doesnt/even/exist" in str(result.exception)

    def test_unexisting_parent_output_path(self):

        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            [
                "--input",
                "./test_data/data3",
                "--output",
                "./test_data/some/folder/data4.json",
                "--is_list",
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, FileNotFoundError)

    def test_unexisting_output_path(self):

        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            [
                "--input",
                "./test_data/data3",
                "--output",
                "./test_data/some/data4.json",
                "--is_list",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists("./test_data/some/data4.json")

    def test_empty_dir(self):
        runner = CliRunner()

        result = runner.invoke(
            concat_data,
            [
                "--input",
                "./test_data/data3",
                "--output",
                "./test_data/data3.json",
                "--is_list",
            ],
        )

        assert result.exit_code == 0

        with open("./test_data/data3.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        assert len(data) == 0


from pandas import read_csv
from ast import literal_eval
from src_rest.transformers.transform_mosdata import *


class TestTransformMosdata:
    @pytest.fixture(autouse=True)
    def init_data(self):
        self.record = {
            "Number": 0,
            "global_id": "12233",
            "Cells": {
                "a": 1,
                "b": 2,
                "geoData": {"coordinates": [0, 1]},
                "PublicPhone": [{"PublicPhone": "1"}, {"PrivatePhone": "2"}],
            },
        }

        safe_mkdir("./test_record")
        with open("./test_record/data.json", "w", encoding="utf-8") as file:
            json.dump([self.record], file)

        yield

        os.system("rm -rf ./test_record")

    def test_process_record(self):

        result = process_record(self.record)

        assert result["Number"] == 0
        assert result["x_coord"] == 0
        assert result["y_coord"] == 1
        assert isinstance(result["PublicPhone"], list)
        assert result["PublicPhone"][1] == "2"

    def test_process_mosdata(self):

        runner = CliRunner()

        runner.invoke(
            process_mosdata,
            [
                "--input",
                "./test_record/data.json",
                "--output",
                "./test_record/output.csv",
            ],
        )

        assert os.path.exists("./test_record/output.csv")
        df = read_csv("./test_record/output.csv")

        assert df.shape[0] == 1
        assert df["Number"][0] == 0
        assert df["x_coord"][0] == 0
        assert df["PublicPhone"].apply(literal_eval)[0][1] == "2"


from numpy import array
from pandas import Series

from src_rest.transformers.utils import *
from src_rest.scrapying.utils import dump_json

from pymorphy2 import MorphAnalyzer


class TestUtils:
    @pytest.fixture(autouse=True)
    def init_data(self):
        safe_mkdir("./test_utils")
        yield
        os.system("rm -rf ./test_utils")

    def test_extract_attribues(self):

        string = "Улица пушкина, дом колотушкина, корпус пичужкина"
        items = string.lower().split(",")

        street_info = extract_patterns(items, STREET_PATTERNS)
        assert street_info["Type"] == "улица"
        assert street_info["Name"] == "пушкина"

        house_info = extract_patterns(items, HOUSE_PATTERNS)
        assert house_info["Type"] == "дом"
        assert house_info["Name"] == "колотушкина"

        building_type = extract_patterns(items, BUIDING_PATTERNS)
        assert building_type["Type"] == "корпус"
        assert building_type["Name"] == "пичужкина"

    def test_extract_attributes_with_none(self):
        string = "переулок пушкина, стр пичужкина"
        items = string.lower().split(",")

        street_info = extract_patterns(items, STREET_PATTERNS)
        assert street_info["Type"] == "переулок"
        assert street_info["Name"] == "пушкина"

        house_info = extract_patterns(items, HOUSE_PATTERNS)
        assert house_info["Type"] is None
        assert house_info["Name"] is None

        building_type = extract_patterns(items, BUIDING_PATTERNS)
        assert building_type["Type"] == "строение"
        assert building_type["Name"] == "пичужкина"

    def test_extract_attributes_false_positives(self):
        string = "караул пушкина, выпад 7, атас пичужкина"
        items = string.lower().split(",")

        street_info = extract_patterns(items, STREET_PATTERNS)
        assert street_info["Type"] is None
        assert street_info["Name"] is None

        house_info = extract_patterns(items, HOUSE_PATTERNS)
        assert house_info["Type"] is None
        assert house_info["Name"] is None

        building_type = extract_patterns(items, BUIDING_PATTERNS)
        assert building_type["Type"] is None
        assert building_type["Name"] is None

    def test_extract_attributes_short(self):
        string = "ул пушкина, д 7, с пичужкина"
        items = string.lower().split(",")

        street_info = extract_patterns(items, STREET_PATTERNS)
        assert street_info["Type"] == "улица"
        assert street_info["Name"] == "пушкина"

        house_info = extract_patterns(items, HOUSE_PATTERNS)
        assert house_info["Type"] == "дом"
        assert house_info["Name"] == "7"

        building_type = extract_patterns(items, BUIDING_PATTERNS)
        assert building_type["Type"] == "строение"
        assert building_type["Name"] == "пичужкина"

    def test_load_process_json(self):

        json_data = {"key": "value", "some_other_key": "value"}

        def _process_json(data: dict, fname: str) -> dict:

            new_json = {
                "key": data["key"][0],
                "some_other_key": data["some_other_key"][0],
            }

            return new_json

        dump_json(json_data, "./test_utils/test.json")

        data = load_process_json("./test_utils/test.json", _process_json)

        assert isinstance(data, dict)
        assert data["key"] == "v"
        assert data["some_other_key"] == "v"

    def test_haversine_distance(self):
        x1 = array([30.0])
        y1 = array([30.0])

        x2 = array([30.0])
        y2 = array([30.0])

        dist = haversine_vectorize(x1, y1, x2, y2)[0]
        assert dist < 1e-5, "Distance between equal points should be 0"

        y1 = array([55.73])
        x1 = array([37.6])

        x2 = array([47.5])
        y2 = array([42.98])

        dist = haversine_vectorize(x1, y1, x2, y2)[0]
        assert dist < 1.6e6, "Distance between equal points should < 2e+6"
        assert dist > 1.5e5, "Distance between equal points should be > 1.5e+6"

    def test_normalize(self):
        sample_seq = ["мама", "готовила", "рыбу"]
        morph = MorphAnalyzer()
        out_seq = normalize(sample_seq, morph)

        assert len(out_seq) == 3
        assert out_seq[0] == "мама"
        assert out_seq[1] == "готовить"
        assert out_seq[2] == "рыба"

    def test_search_words(self):

        seq = ["a", "b", "cd", "a", "cd"]
        words = ["b", "cd"]

        cnt = search_words(seq, words)
        assert cnt["b"] == 1
        assert cnt["cd"] == 2

    def test_preprocess_texts(self):
        texts = [
            "мама готовила рыбу раз",
            "люблю грозу она прекрасна и что",
        ]

        texts_out = preprocess_texts(texts, n_jobs=-1)
        assert len(texts_out) == 2
        assert texts_out[0] == "мама готовить рыба раз"
        assert texts_out[1] == "любить гроза она прекрасный и что"

    def test_clear_texts(self):
        texts = Series(
            [
                "Мама готовила рыбу 55555 раз",
                "Люблю грозу! она прекрасна, и что? ??",
            ]
        )
        texts_out = clear_texts(texts)

        assert len(texts_out) == 2
        assert texts_out[0] == "мама готовила рыбу раз"
        assert texts_out[1] == "люблю грозу она прекрасна и что"

    def test_find_dish_aspects(self):
        texts = [
            "мама готовить рыба шашлык раз".split(),
            "любить гроза она прекрасный и что и сушить".split(),
        ]

        ids = [0, 1]

        aspects = find_dish_aspects(texts, ids)
        assert len(aspects) == 3
        aspects_unique = aspects.value.tolist()
        assert "суши" in aspects_unique
        assert "рыба" in aspects_unique
        assert "шашлык" in aspects_unique

    def test_preprocess_df_text(self):
        texts = [
            "Мама готовила рыбу 55555 раз",
            "Люблю грозу! она прекрасна, и что? ??",
        ]

        data = DataFrame({"text": texts, "other_column": [0, 1]})

        result = preprocess_df_text(data, col_text="text", n_jobs=-1)
        assert result.shape == (2, 9)
        assert result.text_norm.iloc[0] == "мама готовить рыба раз"
        assert result.text_norm.iloc[1] == "любить гроза она прекрасный и что"
        assert result.text_norm.iloc[0] == "мама готовить рыба раз"
        assert "neutral" in result.columns

    def test_score_texts_dostoevsky(self):

        messages = [
            "Сегодня хорошая погода",
            "Я счастлив проводить с тобою время",
            "Мне нравится эта музыкальная композиция",
            "В больнице была ужасная очередь",
            "Сосед с верхнего этажа мешает спать",
            "Маленькая девочка потерялась в торговом центре",
        ]

        scores = score_texts_dostoevsky(messages)

        assert len(scores) == len(messages)
        assert "negative" in scores[0]
        assert "positive" in scores[1]
        assert "neutral" in scores[2]

    def test_calculate_overall_sentiment(self):

        data = DataFrame(
            [
                {
                    "negative": 0,
                    "positive": 0.95,
                    "neutral": 0.6,
                    "skip": 0,
                    "speech": 0,
                }
            ]
        )

        data_sentiment = calculate_overall_sentiment(data)
        assert data_sentiment.sentiment.iloc[-1] > 0

    def test_boxcox_normalize(self):

        data = array([1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 2.5])
        data_normed = boxcox_normalize(data)
        assert data_normed.shape == data.shape
        assert isinstance(data_normed, ndarray)
        assert abs(data_normed.mean()) < 0.05

        series = Series(data)

        series_normed = boxcox_normalize(series, add_one=True)
        assert isinstance(series_normed, Series)


from pandas import NA, read_csv, isna
from src_rest.transformers.transform_mosdata import create_mosdata_datamart


class TestMosdataDatamart:
    @pytest.fixture(autouse=True)
    def init_data(self):
        self.path = "./src/src_rest/tests/data/sample_data.csv"
        self.data = read_csv(self.path)
        safe_mkdir("./dm")
        yield
        os.system("rm -rf ./dm")

    def test_create_mosdata_datamart(self):
        df = create_mosdata_datamart(self.data)
        assert df.shape[0] == 2

        assert isna(df.PublicPhone.iloc[0])
        assert df.Name_norm.iloc[0] == "чебуречная"
        assert df.StreetType.iloc[1] == "бульвар"
        assert df.StreetName.iloc[1] == "сиреневый"
        assert df.HouseType.iloc[1] == "дом"
        assert df.HouseName.iloc[1] == "15а"

    def test_mosdata_datamart(self):
        runner = CliRunner()

        result = runner.invoke(
            mosdata_datamart,
            [
                "--input",
                "./src/src_rest/tests/data/sample_data.csv",
                "--output",
                "./dm/data.csv",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists("./dm/data.csv")

        df = read_csv("./dm/data.csv")
        assert isna(df.PublicPhone.iloc[0])
        assert df.Name_norm.iloc[0] == "чебуречная"
        assert df.StreetType.iloc[1] == "бульвар"
        assert df.StreetName.iloc[1] == "сиреневый"
        assert df.HouseType.iloc[1] == "дом"
        assert df.HouseName.iloc[1] == "15а"


from bs4 import BeautifulSoup
from src_rest.transformers.transform_mos_rest import (
    _extract_value,
    parse_data,
    parse_details,
    parse_item,
    process_mos_rest,
)


class TestTranformMosRest:
    @pytest.fixture(autouse=True)
    def init_data(self):
        safe_mkdir("./mos_rest")
        self.sample_html = """<span class="vcard">
        <a class="clearfix" href="ref.html" title="Name">
        <span class="rate"><span><img class="round-img-nosize" height="33" src="" width="33"/></span></span>
        <span class="col col_1"><span class="fn org">Fn org name</span></span>
        <span class="col col_2">
        </span>
        <span class="col col_3"><span>Cuisine</span></span>
        <span class="col col_4"><span></span></span>
        </a>
        <span class="tel"><span class="value-title" title="phone"></span></span>
        <span class="permalink"><span class="value-title" title="ref"></span></span>
        <span class="adr">
        <span class="locality"><span class="value-title" title="Москва"></span></span>
        <span class="street-address"><span class="value-title" title="address"></span></span>
        <span class="some_class"><span class="nalue-title" title="val"></span></span>
        </span>
        </span>"""
        self.sample_html1 = """<span class="vcard">
        <a class="clearfix" href="ref.html" title="Name">
        <span class="rate"><span><img class="round-img-nosize" height="33" src="" width="33"/></span></span>
        <span class="col col_1"><span class="fn org">Fn org name</span></span>
        <span class="col col_2">
        </span>
        <span class="col col_3"><span>Cuisine</span></span>
        <span class="col col_4"><span></span></span>
        </a>
        <span class="tel"><span class="value-title" title="phone"></span></span>
        <span class="permalink"><span class="value-title" title="ref"></span></span>
        <span class="adr">
        <span class="loc"><span class="value-title" title="Москва"></span></span>
        <span class="street-address"><span class="value-title" title="address"></span></span>
        <span class="some_class"><span class="nalue-title" title="val"></span></span>
        </span>
        </span>"""
        yield
        os.system("rm -rf ./mos_rest")

    def test__extract_value(self):

        soup = BeautifulSoup(self.sample_html, "html.parser")

        result = _extract_value(soup, "tel", False)
        assert result == "phone"

        result = _extract_value(soup, "tel", True)
        assert result == "phone"

        result = _extract_value(soup, "locality", True)
        assert result == "Москва"

        with pytest.raises(ValueError):
            result = _extract_value(soup, "no class", True)

        result = _extract_value(soup, "no class", False)
        assert result is None

        with pytest.raises(ValueError):
            result = _extract_value(soup, "some_class", True)

        result = _extract_value(soup, "some_class", False)
        assert result is None

    def test_parse_item(self):

        result = parse_item(self.sample_html)
        assert result["cuisine"] == "Cuisine"

        with pytest.raises(ValueError):
            parse_item(self.sample_html1)

    def test_parse_data(self):

        data = {
            "ok": True,
            "data": {
                "cards": [self.sample_html],
            },
            "dttm": "dttm",
            "url": "url",
            "fname": "fname",
        }

        result = parse_data(data, "file.json")

        assert isinstance(result, list)
        assert isinstance(result[0], dict)

        assert result[0]["dttm"] == "dttm"
        assert result[0]["cuisine"] == "Cuisine"

        data = {
            "ok": False,
            "data": {
                "cards": [self.sample_html],
            },
            "dttm": "dttm",
            "url": "url",
            "fname": "fname",
        }

        result = parse_data(data, "file.json")
        assert len(result) == 0

    def test_process_mosrest(self):

        safe_mkdir("./mos_rest/test")
        data = {
            "ok": True,
            "data": {
                "cards": [self.sample_html],
            },
            "dttm": "dttm",
            "url": "url",
            "fname": "fname",
        }

        dump_json(data, "./mos_rest/test/test.json")

        runner = CliRunner()

        runner.invoke(
            process_mos_rest,
            [
                "--input",
                "./mos_rest/test/",
                "--output",
                "./mos_rest/output.csv",
                "--n_jobs",
                "-1",
            ],
        )

        assert os.path.exists("./mos_rest/output.csv")

        df = read_csv("./mos_rest/output.csv")

        assert df.shape[0] == 1
        assert df.cuisine.iloc[0] == "Cuisine"

        data = {
            "ok": False,
            "data": {
                "cards": [self.sample_html],
            },
            "dttm": "dttm",
            "url": "url",
            "fname": "fname",
        }

        dump_json(data, "./mos_rest/test/test.json")

        runner = CliRunner()

        runner.invoke(
            process_mos_rest,
            [
                "--input",
                "./mos_rest/test/",
                "--output",
                "./mos_rest/output.csv",
                "--n_jobs",
                "-1",
            ],
        )

        assert os.path.exists("./mos_rest/output.csv")

        df = read_csv("./mos_rest/output.csv")

        assert df.shape[0] == 0

    def test_parse_details(self):

        details = {
            "ok": True,
            "elapsed": 1.204689,
            "status_code": 200,
            "reason": "OK",
            "url": "website.org",
            "encoding": "utf-8",
            "headers": {"key": "value"},
            "dttm": "2022-09-09 20:24:22",
            "sha256": "20df19",
            "data": {
                "x_coord": 0.0,
                "y_coord": 0.0,
                "avg_check": "check",
                "opening_hours": "regime",
                "street_address": "Address",
                "address_locality": "Locality",
                "aspect_stars": {"card": 0},
                "review": "review",
            },
        }

        result = parse_details(details, "file.json")[0]
        assert result["url"] == "website.org"
        assert result["fname"] == "file.json"
        assert result["dttm"] == "2022-09-09 20:24:22"
        assert result["x_coord"] == 0
        assert result["y_coord"] == 0
        assert result["avg_check"] == "check"
        for key in details["data"]:
            assert key in result
        assert isinstance(result["aspect_stars"], str)
