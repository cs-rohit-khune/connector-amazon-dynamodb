config = {
    "config_type": "Access Credentials",
    "aws_region": "us-east-2",
    "aws_access_key": "XXXXXXXXXXXXXXXXXXXX",
    "aws_secret_access_key": "xxxxxxxxxxxxxxxxxxxxxxxx"
}

get_table_list_params = {}

create_table_params = {
    "TableName": "test-intg-2",
    "partitionKeyName": "id",
    "partitionKeyDataType": "Number",
    "billingMode": "Provisioned",
    "readCapacityUnits": 5,
    "writeCapacityUnits": 5
}
update_table_params = {
    "TableName": "test-intg-2",
    "updateOperation": "Modify Provisioned Throughput",
    "billingMode": "Provisioned",
    "readCapacityUnits": 6,
    "writeCapacityUnits": 6
}
delete_table_params = {
    "TableName": "test-intg-2"
}
get_table_details_params = {
    "TableName": "test-intg-2"
}

add_item_params = {
    "TableName": "test-intg-2",
    "Item": {
        "id": {
            "N": "1"
        },
        "name": {
            "S": "intg"
        }
    }
}

delete_item_params = {
    "TableName": "test-intg-2",
    "Key": {
        "id": {
            "N": "1"
        }
    }
}

search_item_params = {
    "TableName": "test-intg-2",
    "Key": {
        "id": {
            "N": "1"
        }
    }
}
# {
#     "TableName": "test-intg-2",
#     "Select": 'ALL_ATTRIBUTES',
#     "KeyConditions": {}
# }

get_global_table_list_params = {}

create_global_table_params = {
    "globalTableName": "test-intg-2",
    "regionName": "us-east-2"
}

create_backup_params = {
    "TableName": "test-intg-2",
    "BackupName": "test-intg-2-backup"
}
get_table_backup_list_params = {}
get_table_backup_details_params = {
    "BackupArn": "arn:aws:dynamodb:us-east-2:514724648128:table/test-intg-2/backup/01665664633831-ff21e1f1",
}
delete_table_backup_params = {
    "BackupArn": "arn:aws:dynamodb:us-east-2:514724648128:table/test-intg-2/backup/01665663331592-e0b24f45",
}
