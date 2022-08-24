from unittest import result
import pytest
import json
import os

from click.testing import CliRunner

from src_rest.loaders.utils import safe_mkdir
from src_rest.transformers.transform import concat_data


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
            ["--input", "./test_data/data2", "--output", "./test_data/data2.json", '--is_list'],
        )

        assert result.exit_code == 0

        with open("./test_data/data2.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        assert len(data) == 10 * 3
        assert data[3] == 1
