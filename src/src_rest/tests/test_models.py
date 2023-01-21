import pytest
from sklearn.base import BaseEstimator

from src_rest.models.models import *


def test_create_model():
    model = create_model()
    assert isinstance(model, BaseEstimator)
