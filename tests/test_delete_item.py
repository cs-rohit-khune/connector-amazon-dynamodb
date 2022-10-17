import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order2
def test_delete_item():
    resp = conn_package.delete_item(config, delete_item_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
