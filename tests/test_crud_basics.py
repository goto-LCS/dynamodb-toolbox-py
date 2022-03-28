import string
import random

import pytest
from typing import Dict, Any

from dynamodb_toolbox_py.entity import Entity
from dynamodb_toolbox_py.exceptions.boto import ObjectNotFoundError


def random_table_name() -> str:
    return 'test_' + ''.join([random.choice(string.hexdigits) for _ in range(0, 8)])


def set_keys(item: Dict[str, Any], uid: str):
    item['pk'] = uid
    item['sk'] = item['pk']
    return item


def create_table(dynamodb_resource, table_name):
    # Create a table that will be used for testing
    return dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 123, 'WriteCapacityUnits': 123}
    )


@pytest.mark.asyncio
async def test_create(dynamodb_resource):
    user_entity = await Entity.create_entity(
        entity_name='user',
        table_name=random_table_name(),
        set_keys=set_keys,
        dynamodb_resource=dynamodb_resource
    )

    await create_table(dynamodb_resource, user_entity.table_name)

    created = await user_entity.create_item(
        {'name': 'test'}
    )

    # Test that the item was created
    assert 'pk' in created
    assert created['name'] == 'test'

    # Find the item in the db and see if it is there
    user = await user_entity.get_item(created['pk'])
    assert user is not None
    assert user['name'] == 'test'


@pytest.mark.asyncio
async def test_replace(dynamodb_resource):
    user_entity = await Entity.create_entity(
        entity_name='user',
        table_name=random_table_name(),
        set_keys=set_keys,
        dynamodb_resource=dynamodb_resource
    )

    await create_table(dynamodb_resource, user_entity.table_name)

    created = await user_entity.create_item(
        {'name': 'test'}
    )

    # Test that the item was created
    assert 'pk' in created
    assert created['name'] == 'test'

    # Update the user
    await user_entity.replace_item(created['pk'], {'name': 'updated name'})

    # Find the item in the db and see if it is there
    user = await user_entity.get_item(created['pk'])
    assert user is not None
    assert user['name'] == 'updated name'


@pytest.mark.asyncio
async def test_delete(dynamodb_resource):
    user_entity = await Entity.create_entity(
        entity_name='user',
        table_name=random_table_name(),
        set_keys=set_keys,
        dynamodb_resource=dynamodb_resource
    )

    await create_table(dynamodb_resource, user_entity.table_name)

    created = await user_entity.create_item(
        {'name': 'test'}
    )

    # Test that the item was created
    assert 'pk' in created
    assert created['name'] == 'test'

    # Delete the item
    await user_entity.delete_item(created['pk'])

    # Find the item in the db and see if it is there
    with pytest.raises(ObjectNotFoundError):
        await user_entity.get_item(created['pk'])