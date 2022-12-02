""" Copyright start
  Copyright (C) 2008 - 2022 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

import json
import boto3
import requests
import datetime
from json import JSONEncoder

from connectors.core.connector import get_logger, ConnectorError

from .constants import *

logger = get_logger('aws-dynamodb')
TEMP_CRED_ENDPOINT = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/{}'


class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()


def _get_temp_credentials(config):
    try:
        aws_iam_role = config.get('aws_iam_role')
        url = TEMP_CRED_ENDPOINT.format(aws_iam_role)
        resp = requests.get(url=url, verify=config.get('verify_ssl'))
        if resp.ok:
            data = json.loads(resp.text)
            return data
        else:
            logger.error(str(resp.text))
            raise ConnectorError("Unable to validate the credentials")
    except Exception as Err:
        logger.exception(Err)
        raise ConnectorError(Err)


def _assume_a_role(data, params, aws_region):
    try:
        client = boto3.client('sts', region_name=aws_region, aws_access_key_id=data.get('AccessKeyId'),
                              aws_secret_access_key=data.get('SecretAccessKey'),
                              aws_session_token=data.get('Token'))
        role_arn = params.get('role_arn')
        session_name = params.get('session_name')
        response = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        aws_region2 = params.get('aws_region')
        aws_session = boto3.session.Session(region_name=aws_region2,
                                            aws_access_key_id=response['Credentials']['AccessKeyId'],
                                            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                            aws_session_token=response['Credentials']['SessionToken'])
        return aws_session
    except Exception as Err:
        logger.exception(Err)
        raise ConnectorError(Err)


def _get_session(config, params):
    try:
        config_type = config.get('config_type')
        assume_role = params.get("assume_role", False)
        if config_type == "AWS Instance IAM Role":
            if not assume_role:
                raise ConnectorError("Please Assume a Role to execute actions")

            aws_region = params.get('aws_region')
            data = _get_temp_credentials(config)
            aws_session = _assume_a_role(data, params, aws_region)
            return aws_session

        else:
            aws_access_key = config.get('aws_access_key')
            aws_region = config.get('aws_region')
            aws_secret_access_key = config.get('aws_secret_access_key')
            if assume_role:
                data = {
                    "AccessKeyId": aws_access_key,
                    "SecretAccessKey": aws_secret_access_key,
                    "Token": None
                }
                aws_session = _assume_a_role(data, params, aws_region)
            else:
                aws_session = boto3.session.Session(region_name=aws_region, aws_access_key_id=aws_access_key,
                                                    aws_secret_access_key=aws_secret_access_key)
            return aws_session
    except Exception as Err:
        raise ConnectorError(Err)


def get_aws_client(config, params, service):
    try:
        aws_session = _get_session(config, params)
        aws_client = aws_session.client(service, verify=config.get('verify_ssl'))
        return aws_client
    except Exception as Err:
        logger.exception(Err)
        raise ConnectorError(Err)


def _convert_csv_str_to_list(list_param):
    if isinstance(list_param, str):
        return list_param.split(',')
    elif isinstance(list_param, list):
        return list_param
    else:
        raise ConnectorError("{} is not in a valid list or csv format".format(list_param))


def _get_attribute_mapping(val, mapping_object):
    if val not in mapping_object.keys():
        return val
    else:
        return mapping_object.get(val)


def _create_attribute_dict(params, dict_object, mapping_object=None):
    if mapping_object is not None:
        result = {k: _get_attribute_mapping(params.get(v), mapping_object) for k, v in dict_object.items()}
    else:
        result = {k: params.get(v) for k, v in dict_object.items()}
    return result


def _get_billing_mode_attribute(params):
    billing_mode = {}
    if params.get('billingMode') == 'Provisioned':
        billing_mode['BillingMode'] = _get_attribute_mapping(params.get('billingMode'), BILL_MODE_MAPPING)
        billing_mode['ProvisionedThroughput'] = _create_attribute_dict(params, PROVISIONED_THROUGHPUT)
    else:
        billing_mode['BillingMode'] = _get_attribute_mapping(params.get('billingMode'), BILL_MODE_MAPPING)
    return billing_mode

def _get_attribute_definition(params):
    attrib_definition = {}
    if params.get('sortKey'):
        attrib_definition['AttributeDefinitions'] = [(_create_attribute_dict(params, item, DATA_TYPE_MAPPING)) for
                                                     item in ATTRIBUTE_DEFINITIONS]
    else:
        attrib_definition['AttributeDefinitions'] = [
            _create_attribute_dict(params, ATTRIBUTE_DEFINITIONS[0], DATA_TYPE_MAPPING)]
    return attrib_definition


def _get_key_schema(params):
    key_schema = {}
    params.update(KEY_ROLE_MAPPING)
    if params.get('sortKey'):
        key_schema['KeySchema'] = [(_create_attribute_dict(params, item)) for item in KEY_SCHEMA]
    else:
        key_schema['KeySchema'] = [_create_attribute_dict(params, KEY_SCHEMA[0])]
    return key_schema


def _get_db_stream_attribute(params):
    db_stream = {}
    if params.get('streamEnabled') == 'Enable':
        params.update({'streamEnabled': True})
        db_stream['StreamSpecification'] = _create_attribute_dict(params, STREAM_SPECIFICATION,
                                                                  STREAM_VIEW_TYPE_MAPPING)
    else:
        db_stream['StreamSpecification'] = {'StreamEnabled': False}
    return db_stream


def _get_projection_attribute(params):
    projection_attrib = {'ProjectionType': _get_attribute_mapping(params.get('projection'), PROJECTION_MAPPING)}
    if params.get('projection') == 'Include':
        projection_attrib['NonKeyAttributes'] = _convert_csv_str_to_list(params.get('nonKeyAttributes'))
    return projection_attrib


def _get_global_secondary_index_attribute(params, action):
    if action == 'delete':
        delete = {'IndexName': params.get('indexName')}
        return {'Delete': delete}
    elif action == 'update':
        update = {'IndexName': params.get('indexName'),
                  'ProvisionedThroughput': _create_attribute_dict(params, PROVISIONED_THROUGHPUT)}
        return {'Update': update}
    elif action == 'create':
        create = {'IndexName': params.get('indexName')}
        create.update(_get_key_schema(params))
        create['ProvisionedThroughput'] = _create_attribute_dict(params, PROVISIONED_THROUGHPUT)
        create['Projection'] = _get_projection_attribute(params)
        return {'Create': create}


def build_create_table_payload(params):
    payload = {'TableName': params.get('TableName')}
    payload.update(_get_attribute_definition(params))
    payload.update(_get_key_schema(params))
    payload.update(_get_billing_mode_attribute(params))
    return payload


def build_update_table_payload(params):
    payload = {'TableName': params.get('TableName')}

    if params.get('updateOperation') == 'Modify Provisioned Throughput':
        payload.update(_get_billing_mode_attribute(params))

    elif params.get('updateOperation') == 'Enable or Disable DynamoDB Streams':
        payload.update(_get_db_stream_attribute(params))

    elif params.get('updateOperation') == 'Create a New Global Secondary Index':
        payload.update(_get_attribute_definition(params))
        payload['GlobalSecondaryIndexUpdates'] = [_get_global_secondary_index_attribute(params, action='create')]

    elif params.get('updateOperation') == 'Update a Global Secondary Index':
        payload['GlobalSecondaryIndexUpdates'] = [_get_global_secondary_index_attribute(params, action='update')]

    elif params.get('updateOperation') == 'Remove a Global Secondary Index':
        payload['GlobalSecondaryIndexUpdates'] = [_get_global_secondary_index_attribute(params, action='delete')]

    return payload


def build_add_item_payload(params):
    payload = {'TableName': params.get('TableName'), 'Item': {}, 'ReturnValues': 'ALL_OLD'}
    payload['Item'].update(
        {
            params.get('partitionKeyName'): {
                _get_attribute_mapping(params.get('partitionKeyDataType'), DATA_TYPE_MAPPING): str(params.get('partitionKeyValue'))
            }
        }
    )
    if params.get('sortKey'):
        payload['Item'].update(
            {
                params.get('sortKeyName'): {
                    _get_attribute_mapping(params.get('sortKeyDataType'), DATA_TYPE_MAPPING): str(params.get('sortKeyValue'))
                }
            }
        )
    if params.get('additionalAttributes'):
        payload['Item'].update(params.get('additionalAttributes'))
    return payload


def build_delete_or_search_item_payload(params):
    payload = {'TableName': params.get('TableName'), 'Key': {}, 'ReturnValues': 'ALL_OLD'}
    payload['Key'].update({params.get('partitionKeyName'): {_get_attribute_mapping(params.get('partitionKeyDataType'), DATA_TYPE_MAPPING): str(params.get('partitionKeyValue'))}})
    if params.get('sortKey'):
        payload['Key'].update({params.get('sortKeyName'): {_get_attribute_mapping(params.get('sortKeyDataType'), DATA_TYPE_MAPPING): str(params.get('sortKeyValue'))}})
    if params.get('additionalAttributes'):
        payload['Key'].update(params.get('additionalAttributes'))
    return payload


def build_create_global_table_payload(params):
    payload = {'GlobalTableName': params.get('globalTableName'), 'ReplicationGroup': []}
    payload['ReplicationGroup'].append({'RegionName': params.get('regionName')})
    return payload
