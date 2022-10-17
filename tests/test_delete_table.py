import importlib
import json
import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order2
def test_delete_table():
    resp = conn_package.delete_table(config, delete_table_params)
    print(json.dumps(resp, indent=2))
