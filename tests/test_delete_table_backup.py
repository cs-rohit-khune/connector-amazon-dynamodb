import importlib
import json
import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order2
def test_delete_table_backup():
    resp = conn_package.delete_table_backup(config, delete_table_backup_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
