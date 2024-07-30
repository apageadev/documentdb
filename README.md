# DocumentDB

[![codecov](https://codecov.io/github/apageadev/documentdb/graph/badge.svg?token=WNTC7Q8PRP)](https://codecov.io/github/apageadev/documentdb)

DocumentDB is a JSON document-oriented database built on top of SQLite. It offers a simple and efficient way to store, retrieve, and manage JSON documents using the robust and widely-used SQLite as its storage backend. DocumentDB provides a flexible and schema-less data model, allowing for easy integration and usage in various applications.

## Features

- **JSON Document Storage**: Store JSON documents in a schema-less database.
- **SQLite Backend**: Leverage the reliability and performance of SQLite.
- **Simple API**: Intuitive and easy-to-use API for managing documents.
- **Flexible Querying**: Support for complex queries on JSON data.
- **Lightweight**: Minimal dependencies and lightweight installation.
- **Transactional**: Support for ACID transactions through SQLite.

## Installation

To install DocumentDB, you can use pip:

```sh
pip install documentdb
```

## Usage & Examples

### Creating a Store

A Store is a container for Collections.

To create a Store, you can simply instantiate it with a path to where the store should be written on disk (this path is the underlying SQLite path)

```python
from documentdb import Store

store = Store("path/to/store.db")
```

### Creating a Collection

A Collection is a group of Documents. To create a Collection, you can use the `create_collection` method on the Store instance.

```python
collection = await store.create_collection("animals")
```

### Inserting a Document

A Document is any JSON serializable object. That can be a dictionary, list, string, number, etc, it just must be JSON serializable.

**Example:**
```python
document = {
    "name": "Blueberry",
    "type": "dog",
    "breed": "Maltese",
    "age": 4
}

await collection.insert(document)
```

### Inserting Multiple Documents (Bulk Insert)

You can insert multiple documents at once using the `insert_many` method.

**Example:**

This is a small list for brevity when in reality you would likely have a much larger list of documents to insert.

```python
documents = [
    {
        "name": "Blueberry",
        "type": "dog",
        "breed": "Maltese",
        "age": 4
    },
    {
        "name": "Luna",
        "type": "dog",
        "breed": "Corgi",
        "age": 2
    }
]

await collection.insert_many(documents)
```