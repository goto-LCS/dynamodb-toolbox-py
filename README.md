# DynamoDB Toolbox for Python

## Single Table Designs for Python has never been easier! <!-- omit in toc -->

The **DynamoDB Toolbox for Python** is a collection of tools for working with DynamoDB. It is designed with single table design in mind, but it works perfectly for multi-table set-ups.

By configuring **entities** in your code you can automatically create, update, patch, delete, and query your data, no complicated and verbose boto3 calls to be manually set up, this library will do all the hard work for you.

On top of being just developer friendly, it has excellent features.

## Features

- Integration with [Pydantic](https://pydantic-docs.helpmanual.io/) models (ideal for [FastAPI](https://fastapi.tiangolo.com/) applications)
- Asynchronous by design
- Super simple CRUD functions
- Queries and pagination support
- Assign aliases to your entities to make them more readable while keeping data on the db with a lower footprint

## Installation

```
pip install dynamodb-toolbox-py
```

Done!

## Usage

...