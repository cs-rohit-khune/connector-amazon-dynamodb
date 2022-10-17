import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order1
def test_search_item():
    resp = conn_package.search_item(config, search_item_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
