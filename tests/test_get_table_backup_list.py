import importlib
import json

import pytest

from .data import *

conn_package = importlib.import_module("aws-dynamodb.operations")


@pytest.mark.order1
def test_get_table_backup_list():
    resp = conn_package.get_table_backup_list(config, get_table_backup_list_params)
    print(json.dumps(resp, indent=2, cls=conn_package.DateTimeEncoder))
