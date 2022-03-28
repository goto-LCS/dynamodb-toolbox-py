import uuid
from typing import Any, Dict, Callable

from aioboto3.session import ResourceCreatorContext
from boto3.dynamodb.conditions import Key

from dynamodb_toolbox_py.exceptions.boto import handle_botocore_exceptions, ObjectNotFoundError


class Entity:
    """
    High-level wrapper around dynamodb with all basic operations
    needed for most CRUD and query operations
    """
    table_name: str
    entity_name: str
    _set_keys: Callable
    _dynamodb_resource: ResourceCreatorContext

    @classmethod
    async def create_entity(cls, table_name: str, entity_name: str, set_keys: Callable, dynamodb_resource: ResourceCreatorContext):
        self = cls()
        self.table_name = table_name
        self.entity_name = entity_name
        self._set_keys = set_keys
        self._dynamodb_resource = dynamodb_resource
        self._table = await self._dynamodb_resource.Table(self.table_name)
        return self

    @handle_botocore_exceptions()
    async def create_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Puts an item on the DB
        """
        self._set_keys(item, uid=f'{self.entity_name}_{uuid.uuid4()}')

        await self._table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#key)",
            ExpressionAttributeNames={"#key": 'pk'},
        )

        return item

    @handle_botocore_exceptions()
    async def replace_item(self, uid: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replaces an item on the DB (update operation)
        """
        self._set_keys(item, uid)

        await self._table.put_item(
            Item=item,
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={"#key": 'pk'},
        )

        return item

    @handle_botocore_exceptions()
    async def patch_item(self, uid: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Patches an item on the DB
        """
        await self._table.update_item(
            Key={
                'pk': uid,
                'sk': uid
            },
            UpdateExpression='set ' + ' '.join(
                [f'{field}=:{field}' for field in item.keys()]
            ),
            ExpressionAttributeValues={
                f':{field}': value for field, value in item.items()
            },
            ConditionExpression="attribute_exists(#key)",
            ExpressionAttributeNames={"#key": 'pk'},
        )

        return item

    @handle_botocore_exceptions()
    async def get_item(self, uid) -> Dict[str, Any]:
        """
        Gets an item from the db
        """
        response = await self._table.get_item(Key={
            'pk': uid, 'sk': uid
        })
        if item := response.get("Item"):
            return item
        else:
            raise ObjectNotFoundError(
                f'Object "{self.entity_name}" with uid "{uid}" not found!'
            )

    @handle_botocore_exceptions()
    async def query_index(self,
                          index_name: str,
                          pk_field: str,
                          pk_val: str,
                          page_size: int,
                          last_evaluated_uid: str = None
                          ):
        """
        Queries an index on the DB by the given index, fields and pagging attributes
        """
        if last_evaluated_uid is not None:
            last_evaluated_key = self._set_keys({}, last_evaluated_uid)
            response = await self._table.query(
                IndexName=index_name,
                KeyConditionExpression=Key(pk_field).eq(pk_val),
                ExclusiveStartKey=last_evaluated_key,
                Limit=page_size
            )
        else:
            response = await self._table.query(
                IndexName=index_name,
                KeyConditionExpression=Key(pk_field).eq(pk_val),
                Limit=page_size
            )

        return {
            'items': response['Items'],
            'last_evaluated_uid': response['LastEvaluatedKey']['pk'] if 'LastEvaluatedKey' in response else None
        }

    @handle_botocore_exceptions()
    async def delete_item(self, uid: str):
        """
        Removes an item from the DB
        """
        await self._table.delete_item(
            Key={
                'pk': uid,
                'sk': uid
            }
        )
