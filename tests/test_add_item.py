import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order1
def test_add_item():
    resp = conn_package.add_item(config, add_item_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
