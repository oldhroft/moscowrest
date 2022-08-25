import os
import pytest

from src_rest.loaders.data_loaders_api import *
from src_rest.loaders.utils import *


class TestUtils:
    @pytest.fixture(autouse=True)
    def init_data(self):
        os.mkdir("./test_path")
        yield
        os.system("rm -rf ./test_path")

    def test_create_mkdir(self):

        safe_mkdir("./test_path/path")
        assert os.path.exists("./test_path/path")

        with pytest.raises(FileNotFoundError):
            safe_mkdir("./test_path/path1/path")

    def test_check_paths(self):
        check_paths("./test_path", "./test_path")
        check_paths("./test_path", "./test_path/path1/data.json")
        assert os.path.exists("./test_path/path1")

        with pytest.raises(FileNotFoundError):
            check_paths("./test_path/path2", "./test_path/path3/data.json")

        with pytest.raises(FileNotFoundError):
            check_paths("./test_path/", "./test_path/path4/path5/data.json")
