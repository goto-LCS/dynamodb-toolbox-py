import os

import pytest
from aioboto3 import Session
from aioboto3.session import ResourceCreatorContext


@pytest.fixture(scope="function")
async def dynamodb_resource(aws_credentials, dynamodb2_server: str) -> ResourceCreatorContext:
    session = Session()
    async with session.resource('dynamodb', endpoint_url=dynamodb2_server) as resource:
        yield resource


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'


pytest_plugins = ['tests.mock_server']