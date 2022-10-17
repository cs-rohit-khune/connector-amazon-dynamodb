import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order1
def test_create_table():
    resp = conn_package.create_table(config, create_table_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
