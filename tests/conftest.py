import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl_process import CSVLoader


@pytest.fixture
def csv_loader():
    return CSVLoader()
