import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order1
def test_update_table():
    resp = conn_package.update_table(config, update_table_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))