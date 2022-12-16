""" Copyright start
  Copyright (C) 2008 - 2022 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

from .utils import *
from .utils import _get_temp_credentials

logger = get_logger('aws-dynamodb')


def remove_unwanted_param(params):
    params.pop('aws_region', None)
    params.pop('assume_role', None)
    params.pop('session_name', None)
    params.pop('role_arn', None)
    param_dict = {k: v for k, v in params.items() if v is not None and v != '' and v != {} and v != []}
    return param_dict


def create_table(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_create_table_payload(params)
    response = client.create_table(**payload)
    response.pop('ResponseMetadata')
    return response


def delete_table(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.delete_table(**payload)
    response.pop('ResponseMetadata')
    return response


def update_table(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_update_table_payload(params)
    response = client.update_table(**payload)
    response.pop('ResponseMetadata')
    return response


def get_table_list(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    response = client.list_tables()
    response.pop("ResponseMetadata")
    return response


def get_table_details(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.describe_table(**payload)
    response.pop('ResponseMetadata')
    return response


def create_or_update_table_item(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_add_item_payload(params)
    response = client.put_item(**payload)
    response.pop('ResponseMetadata')
    return response


def delete_item(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_delete_or_search_item_payload(params)
    response = client.delete_item(**payload)
    response.pop('ResponseMetadata')
    return response


def search_item(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_delete_or_search_item_payload(params)
    response = client.get_item(**payload)
    response.pop('ResponseMetadata')
    return response


def create_global_table(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = build_create_global_table_payload(params)
    response = client.create_global_table(**payload)
    response.pop('ResponseMetadata')
    return response


def get_global_table_details(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    response = client.describe_global_table(GlobalTableName=params.get('globalTableName'))
    response.pop('ResponseMetadata')
    return response


def get_global_table_list(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    response = client.list_global_tables()
    response.pop('ResponseMetadata')
    return response


def create_backup(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.create_backup(**payload)
    response.pop('ResponseMetadata')
    return response


def get_table_backup_details(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.describe_backup(**payload)
    response.pop('ResponseMetadata')
    return response


def get_table_backup_list(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.list_backups(**payload)
    response.pop('ResponseMetadata')
    return response


def delete_table_backup(config, params):
    client = get_aws_client(config, params, DYNAMODB_SERVICE)
    payload = remove_unwanted_param(params)
    response = client.delete_backup(**payload)
    response.pop('ResponseMetadata')
    return response


def check_health(config):
    try:
        config_type = config.get('config_type')
        if config_type == "AWS Instance IAM Role":
            if _get_temp_credentials(config):
                return True
            else:
                logger.error("Invalid Role. Please verify that the role is associated with your instance.")
                raise ConnectorError("Invalid Role. Please verify that the role is associated with your instance.")
        else:
            aws_access_key = config.get('aws_access_key')
            aws_region = config.get('aws_region')
            aws_secret_access_key = config.get('aws_secret_access_key')
            client = boto3.client(DYNAMODB_SERVICE, region_name=aws_region, aws_access_key_id=aws_access_key,
                                  aws_secret_access_key=aws_secret_access_key)
            response = client.list_tables(Limit=1)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return True

    except Exception as Err:
        logger.exception(Err)
        raise ConnectorError(Err)


operations = {
    'create_table': create_table,
    'delete_table': delete_table,
    'update_table': update_table,
    'get_table_list': get_table_list,
    'get_table_details': get_table_details,
    'create_or_update_table_item': create_or_update_table_item,
    'delete_item': delete_item,
    'search_item': search_item,
    'create_global_table': create_global_table,
    'get_global_table_details': get_global_table_details,
    'get_global_table_list': get_global_table_list,
    'create_backup': create_backup,
    'get_table_backup_list': get_table_backup_list,
    'get_table_backup_details': get_table_backup_details,
    'delete_table_backup': delete_table_backup
}
