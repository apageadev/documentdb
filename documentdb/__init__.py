import re
from ujson import loads, dumps
import typing
from pathlib import Path
from databases import Database

COLLECTION_PREFIX = "col_"


class CollectionNotFound(Exception):
    """
    raised when a collection is not found
    """

    pass


class CollectionAlreadyExists(Exception):
    """
    raised when a collection with the same name already exists
    """

    pass


class InvalidCollectionName(Exception):
    """
    raised when the collection name contains invalid characters
    """

    pass


class ViewNotFound(Exception):
    """
    raised when a view is not found
    """

    pass


class ViewAlreadyExists(Exception):
    """
    raised when a view with the same name already exists
    """

    pass


class InvalidViewName(Exception):
    """
    raised when the view name contains invalid characters
    """

    pass


class RecordNotFound(Exception):
    """
    raised when a record is not found
    """

    pass


class InvalidOperator(Exception):
    """
    raised when an invalid operator is used in a query
    """

    pass


def build_condition(
    key: str, value: typing.Any, use_json_extract: bool = True
) -> typing.Tuple[str, typing.Dict[str, typing.Any]]:
    def format_condition(
        json_key: str, operator: str, val: typing.Any
    ) -> typing.Tuple[str, typing.Any]:
        param_key = f"{key.replace('.', '_')}_{operator}"
        if operator == "eq":
            return f"{json_key} = :{param_key}", {param_key: val}
        elif operator == "gt":
            return f"{json_key} > :{param_key}", {param_key: val}
        elif operator == "gte":
            return f"{json_key} >= :{param_key}", {param_key: val}
        elif operator == "lt":
            return f"{json_key} < :{param_key}", {param_key: val}
        elif operator == "lte":
            return f"{json_key} <= :{param_key}", {param_key: val}
        elif operator == "sw":
            return f"{json_key} LIKE :{param_key}", {param_key: f"{val}%"}
        elif operator == "ew":
            return f"{json_key} LIKE :{param_key}", {param_key: f"%{val}"}
        elif operator == "contains":
            return f"{json_key} LIKE :{param_key}", {param_key: f"%{val}%"}
        elif operator == "in":
            placeholders = ", ".join([f":{param_key}_{i}" for i, _ in enumerate(val)])
            params = {f"{param_key}_{i}": v for i, v in enumerate(val)}
            return f"{json_key} IN ({placeholders})", params
        elif operator == "swci":
            return f"LOWER({json_key}) LIKE LOWER(:{param_key})", {param_key: f"{val}%"}
        elif operator == "ewci":
            return f"LOWER({json_key}) LIKE LOWER(:{param_key})", {param_key: f"%{val}"}
        else:
            raise InvalidOperator(f"invalid operator: {operator}")

    if not use_json_extract:
        key = key.replace(".", "_")

    if isinstance(value, dict):
        conditions = []
        parameters = {}
        for op, val in value.items():
            json_key = (
                key.replace(".", "_")
                if not use_json_extract
                else f"json_extract(data, '$.{key}')"
            )
            condition, param = format_condition(json_key, op, val)
            conditions.append(condition)
            parameters.update(param)
        return " AND ".join(conditions), parameters
    else:
        json_key = (
            key.replace(".", "_")
            if not use_json_extract
            else f"json_extract(data, '$.{key}')"
        )
        condition, param = format_condition(json_key, "eq", value)
        return condition, param


def parse_query(
    query: dict, use_json_extract: bool = True
) -> typing.Tuple[str, typing.Dict[str, typing.Any]]:
    conditions = []
    parameters = {}

    def enclose_key(key: str) -> str:
        if "." in key:
            return ".".join([f"`{part}`" for part in key.split(".")])
        return f"{key}"

    if "AND" in query or "OR" in query:
        for conjunction in ("AND", "OR"):
            if conjunction in query:
                sub_conditions = []
                for subquery in query[conjunction]:
                    sub_condition, sub_params = parse_query(subquery, use_json_extract)
                    sub_conditions.append(f"({sub_condition})")
                    parameters.update(sub_params)
                condition = f" {conjunction} ".join(sub_conditions)
                conditions.append(condition)
    else:
        for key, value in query.items():
            condition, params = build_condition(
                key, value, use_json_extract=use_json_extract
            )
            # Enclose column names with backticks but not parameter names
            enclosed_condition = condition.replace(key, enclose_key(key))
            conditions.append(enclosed_condition)
            parameters.update(params)
        condition = " AND ".join(conditions)

    return " AND ".join(conditions), parameters


async def get_db(name: str) -> Database:
    """
    utility function to get the database connection
    """
    db = Database(f"sqlite+aiosqlite:///{name}.db")
    await db.connect()
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA cache_size=10000;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute("PRAGMA busy_timeout=5000;")
    await db.execute("PRAGMA legacy_alter_table=ON;")
    return db


class View:
    """
    a view within the DocumentDB
    """

    def __init__(self, name: str):
        self.name = name

    async def __init(self, db_name: str):
        self.db = await get_db(db_name)

    @classmethod
    async def init(cls, name: str, db_name: str):
        instance = cls(name)
        await instance.__init(db_name)
        return instance

    async def count(self) -> int:
        """
        returns the number of records in the view
        """
        query = f"SELECT COUNT(*) as num_records FROM `{self.name}`;"
        count = await self.db.fetch_one(query)
        return count["num_records"]

    async def list(self, limit: int = 10, offset: int = 0) -> typing.List[dict]:
        """
        lists all records in the view, paginated.
        """
        query = f"SELECT * FROM `{self.name}` LIMIT :limit OFFSET :offset;"
        values = {"limit": limit, "offset": offset}
        records = await self.db.fetch_all(query, values)
        return [record for record in records]

    async def drop(self):
        """
        Drops the view from the database.
        """
        drop_view_query = f"DROP VIEW {self.name};"
        await self.db.execute(drop_view_query)

    async def rename(self, new_name: str):
        """
        Renames a view in the apagea store.

        Args:
            old_name (str): The current name of the view.
            new_name (str): The new name for the view.

        Raises:
            ValueError: If the old name or new name is invalid.
            Exception: If renaming the view fails.
        """
        try:
            # Retrieve the view definition
            query = (
                "SELECT sql FROM sqlite_master WHERE type='view' AND name=:old_name;"
            )
            result = await self.db.fetch_one(query, values={"old_name": self.name})

            if result is None:
                raise Exception(f"View `{self.name}` does not exist.")

            view_definition = result["sql"]

            # Modify the view definition with the new name
            new_view_definition = view_definition.replace(
                f"CREATE VIEW `{self.name}`", f"CREATE VIEW `{new_name}`"
            )

            # Execute the operations within a transaction
            async with self.db.transaction():
                # Drop the old view
                drop_query = f"DROP VIEW `{self.name}`;"
                await self.db.execute(drop_query)

                # Create the new view with the modified definition
                await self.db.execute(new_view_definition)
                self.name = new_name

        except Exception as e:
            raise Exception(
                f"Failed to rename view from `{self.name}` to `{new_name}`: {str(e)}"
            )

    async def find(
        self, fields: typing.List[str], query: dict, limit: int = 10, offset: int = 0
    ) -> typing.List[dict]:
        """
        Finds records in the view that match the query.
        """
        condition, params = parse_query(query, use_json_extract=False)
        field_expressions = [f"`{field.replace('.', '_')}`" for field in fields]
        select_clause = ", ".join(field_expressions)
        sql_query = f"SELECT {select_clause} FROM `{self.name}` WHERE {condition} LIMIT :limit OFFSET :offset;"
        params["limit"] = limit
        params["offset"] = offset
        return await self.db.fetch_all(sql_query, params)


class Collection:
    """
    a collection within the DocumentDB
    """

    def __init__(self, name: str):
        self.name = name

    async def __init(self, db_name: str):
        self.db = await get_db(db_name)

    @classmethod
    async def init(cls, name: str, db_name: str):
        instance = cls(name)
        await instance.__init(db_name)
        return instance

    async def count(self) -> int:
        """
        returns the number of records in the collection
        """
        query = f"SELECT COUNT(*) as num_records FROM `{COLLECTION_PREFIX}{self.name}`;"
        count = await self.db.fetch_one(query)
        return count["num_records"]

    async def insert(self, pk: str, data: dict):
        """
        creates a new record in the collection
        """
        query = f"INSERT INTO `{COLLECTION_PREFIX}{self.name}` (pk, data) VALUES (:pk, :data);"
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}
        await self.db.execute(query, values)

    async def insert_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        creates multiple records in the collection
        """
        query = f"INSERT INTO `{COLLECTION_PREFIX}{self.name}` (pk, data) VALUES (:pk, :data);"
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await self.db.execute_many(query, values)

    async def upsert(self, pk: str, data: dict):
        """
        will insert a new record if it does not exist,
        otherwise it will update the existing record
        """
        query = f"INSERT OR REPLACE INTO `{COLLECTION_PREFIX}{self.name}` (pk, data) VALUES (:pk, :data);"
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}
        await self.db.execute(query, values)

    async def upsert_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        will insert multiple records if they do not exist,
        otherwise it will update the existing records
        """
        query = f"INSERT OR REPLACE INTO `{COLLECTION_PREFIX}{self.name}` (pk, data) VALUES (:pk, :data);"
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await self.db.execute_many(query, values)

    async def get(self, pk: str, include_pk: bool = False) -> dict:
        """
        retrieves a record from the collection by primary key
        """
        query = f"SELECT * FROM `{COLLECTION_PREFIX}{self.name}` WHERE pk = :pk;"
        values = {"pk": pk}
        record = await self.db.fetch_one(query, values)
        if record is None:
            raise RecordNotFound(f"record with pk '{pk}' not found")
        if include_pk:
            return {record["pk"]: loads(record["data"])}
        return loads(record["data"])

    async def get_many(
        self, pks: typing.List[str], include_pk: bool = False
    ) -> typing.List[dict]:
        """
        Retrieves multiple records from the collection by primary key.
        """
        placeholders = ", ".join([f":pk{i}" for i in range(len(pks))])
        query = f"SELECT * FROM `{COLLECTION_PREFIX}{self.name}` WHERE pk IN ({placeholders});"
        values = {f"pk{i}": pk for i, pk in enumerate(pks)}

        try:
            records = await self.db.fetch_all(query, values)
            if not include_pk:
                return [loads(record["data"]) for record in records]
            return {record["pk"]: loads(record["data"]) for record in records}

        except Exception as e:
            raise RuntimeError(f"Failed to fetch records: {e}")

    async def update(self, pk: str, data: dict):
        """
        Updates an existing record in the collection.
        """
        query = (
            f"UPDATE `{COLLECTION_PREFIX}{self.name}` SET data = :data WHERE pk = :pk;"
        )
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}

        try:
            await self.db.execute(query, values)
        except Exception as e:
            raise RuntimeError(f"Failed to update record: {e}")

    async def update_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        updates multiple existing records in the collection
        """
        query = (
            f"UPDATE `{COLLECTION_PREFIX}{self.name}` SET data = :data WHERE pk = :pk;"
        )
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await self.db.execute_many(query, values)

    async def delete(self, pk: str):
        """
        deletes a record from the collection by primary key
        """
        query = f"DELETE FROM `{COLLECTION_PREFIX}{self.name}` WHERE pk = :pk;"
        values = {"pk": pk}
        await self.db.execute(query, values)

    async def delete_many(self, pks: typing.List[str]):
        """
        Deletes multiple records from the collection by primary key.
        """
        placeholders = ", ".join([f":pk{i}" for i in range(len(pks))])
        query = f"DELETE FROM `{COLLECTION_PREFIX}{self.name}` WHERE pk IN ({placeholders});"
        values = {f"pk{i}": pk for i, pk in enumerate(pks)}

        try:
            await self.db.execute(query, values)
        except Exception as e:
            raise RuntimeError(f"Failed to delete records: {e}")

    async def list(
        self, limit: int = 10, offset: int = 0, include_pk: bool = False
    ) -> typing.List[dict]:
        """
        lists all records in the collection, paginated.
        The pk is included in the dictionary as `_pk`.
        """
        query = f"SELECT * FROM `{COLLECTION_PREFIX}{self.name}` LIMIT :limit OFFSET :offset;"
        values = {"limit": limit, "offset": offset}
        records = await self.db.fetch_all(query, values)
        if not include_pk:
            return [loads(record["data"]) for record in records]
        return [{record["pk"]: loads(record["data"])} for record in records]

    async def find(
        self, query: dict, limit: int = 10, include_pk: bool = False
    ) -> typing.List[dict]:
        """
        Finds records in the collection that match the query.
        """
        condition, params = parse_query(query)
        sql_query = f"SELECT * FROM `{COLLECTION_PREFIX}{self.name}` WHERE {condition} LIMIT :limit;"
        params["limit"] = limit
        try:
            records = await self.db.fetch_all(query=sql_query, values=params)
            if not include_pk:
                return [loads(record["data"]) for record in records]
            return [{"pk": record["pk"], **loads(record["data"])} for record in records]
        except Exception as e:
            raise RuntimeError(f"Failed to find records: {e}")


class DocumentDB:
    """
    a lightewight async storage utility that uses SQLite as it's backend for storing, querying and managing JSON data.
    """

    def __init__(self, name):
        self.name = name

    async def conn(self):
        self.db = await get_db(self.name)

    async def __list_tables_in_db(self):
        """
        lists all tables in the database
        """
        tables = await self.db.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        return [table["name"] for table in tables]

    async def __list_views_in_db(self):
        """
        lists all views in the database
        """
        tables = await self.db.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='view';"
        )
        return [table["name"] for table in tables]

    async def collection_exists(self, collection_name: str) -> bool:
        """
        checks if the store exists
        """
        _tables = await self.__list_tables_in_db()
        return f"{COLLECTION_PREFIX}{collection_name}" in _tables

    async def create_collection(self, collection_name: str) -> Collection:
        """
        creates a collection to store data within the apagea store
        """

        # ensure the collection name is between 2 and 16 characters
        if len(collection_name) < 16 and len(collection_name) > 2:
            InvalidCollectionName("collection name must be between 2 and 16 characters")

        # must be alphanumeric with no spaces. Underscores and hyphens are allowed.
        pattern = re.compile(r"^[a-zA-Z0-9_-]+$")
        if not pattern.match(collection_name):
            raise InvalidCollectionName(
                f"collection name must be alphanumeric with no spaces. Underscores and hyphens are allowed."
            )

        # check if the collection already exists
        _tables = await self.__list_tables_in_db()
        if collection_name in _tables:
            raise CollectionAlreadyExists(
                f"collection with name {collection_name} already exists"
            )

        create_collection_query = f"""
        CREATE TABLE IF NOT EXISTS `{COLLECTION_PREFIX}{collection_name}` (
        `pk` TEXT PRIMARY KEY NOT NULL,
        `data` TEXT CHECK (json_valid(`data`)),
        `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""
        await self.db.execute(create_collection_query)
        return await Collection.init(name=collection_name, db_name=self.name)

    async def delete_collection(self, collection_name: str):
        """
        removes a collection from the apagea store, and all data within it.
        """
        drop_collection_query = (
            f"DROP TABLE IF EXISTS `{COLLECTION_PREFIX}{collection_name}`;"
        )
        await self.db.execute(drop_collection_query)

    async def list_collections(self) -> typing.List[Collection]:
        """
        lists all collections in the apagea store
        """
        _tables = await self.__list_tables_in_db()
        collections = [
            table.replace(COLLECTION_PREFIX, "")
            for table in _tables
            if COLLECTION_PREFIX in table
        ]
        return [
            await Collection.init(name=collection, db_name=self.name)
            for collection in collections
        ]

    async def rename_collection(self, old_name: str, new_name: str):
        """
        renames a collection in the apagea store
        """
        await self.db.execute(
            f"ALTER TABLE `{COLLECTION_PREFIX}{old_name}` RENAME TO `{COLLECTION_PREFIX}{new_name}`;"
        )

    async def get_collection(
        self, collection_name: str, auto_create: bool = False
    ) -> Collection:
        """
        retrieves a collection by name
        """
        _tables = await self.__list_tables_in_db()
        if f"{COLLECTION_PREFIX}{collection_name}" not in _tables:
            if auto_create:
                return await self.create_collection(collection_name)
            raise CollectionNotFound(
                f"collection with name {collection_name} not found"
            )
        return await Collection.init(name=collection_name, db_name=self.name)

    async def destroy(self):
        """destroys the store, and all data within it"""
        assert Path(f"{self.name}.db").exists(), "store does not exist"
        Path(f"{self.name}.db").unlink()

    async def create_view(
        self, view_name: str, fields: typing.List[str], query: dict
    ) -> View:
        # Validate view name
        if not re.match(r"^[a-zA-Z0-9_-]+$", view_name):
            raise InvalidViewName(
                "View name must be alphanumeric with no spaces. Underscores and hyphens are allowed."
            )

        # Ensure all fields belong to the same collection
        collections = set(field.split(".")[0] for field in fields)
        if len(collections) != 1:
            raise ValueError("All fields must belong to the same collection.")
        collection = collections.pop()

        # Validate the collection
        if not await self.collection_exists(collection):
            raise CollectionNotFound(f"Collection '{collection}' not found.")

        # Construct field expressions
        field_expressions = [
            f"json_extract(`{COLLECTION_PREFIX}{collection}`.`data`, '$.{field.split('.')[1]}') AS `{field.replace('.', '_')}`"
            for field in fields
        ]
        select_clause = ", ".join(field_expressions)

        # Construct the FROM clause
        from_clause = f"`{COLLECTION_PREFIX}{collection}`"

        # Construct the WHERE clause
        condition, params = parse_query(query)
        for param, value in params.items():
            if isinstance(value, str):
                value = f"'{value}'"
            condition = condition.replace(f":{param}", str(value))
        where_clause = f"WHERE {condition}" if condition else ""

        # Construct the final SQL query
        create_view_query = f"""
        CREATE VIEW `{view_name}` AS
        SELECT {select_clause}
        FROM {from_clause}
        {where_clause};
        """
        # Execute the SQL statement to create the view
        try:
            await self.db.execute(create_view_query)
            return await View.init(name=view_name, db_name=self.name)
        except Exception as e:
            raise RuntimeError(f"Failed to create view: {e}")

    async def view_exists(self, view_name: str) -> bool:
        """
        checks if the store exists
        """
        _views = await self.__list_views_in_db()
        return view_name in _views

    async def get_view(self, view_name: str) -> View:
        """
        retrieves a view by name
        """
        _views = await self.__list_views_in_db()
        if view_name not in _views:
            raise ViewNotFound(f"view with name {view_name} not found")
        return await View.init(name=view_name, db_name=self.name)

    async def list_views(self) -> typing.List[View]:
        """
        lists all views in the apagea store
        """
        _views = await self.__list_views_in_db()
        return [await View.init(name=view, db_name=self.name) for view in _views]
