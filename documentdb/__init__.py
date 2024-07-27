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


def build_condition(key: str, value: typing.Any) -> str:
    """
    Helper function to build SQL condition based on the query value.
    """
    if isinstance(value, dict):
        conditions = []
        for op, val in value.items():
            json_key = f"json_extract(data, '$.{key}')"
            if op == "gt":
                conditions.append(f"{json_key} > {val}")
            elif op == "gte":
                conditions.append(f"{json_key} >= {val}")
            elif op == "lt":
                conditions.append(f"{json_key} < {val}")
            elif op == "lte":
                conditions.append(f"{json_key} <= {val}")
            elif op == "eq":
                conditions.append(f"{json_key} = '{val}'")
            elif op == "sw":
                conditions.append(f"{json_key} LIKE '{val}%'")
            elif op == "ew":
                conditions.append(f"{json_key} LIKE '%{val}'")
            elif op == "contains":
                conditions.append(f"{json_key} LIKE '%{val}%'")
            elif op == "in":
                conditions.append(f"{json_key} IN ({', '.join(map(str, val))})")
            elif op == "between":
                conditions.append(f"{json_key} BETWEEN {val[0]} AND {val[1]}")
            else:
                raise ValueError(f"Unsupported operator: {op}")
        return " AND ".join(conditions)
    else:
        json_key = f"json_extract(data, '$.{key}')"
        return f"{json_key} = '{value}'"


def parse_query(query: dict) -> str:
    """
    Helper function to parse the query dict into SQL conditions.
    """
    if "AND" in query or "OR" in query:
        conditions = []
        if "AND" in query:
            for subquery in query["AND"]:
                condition = parse_query(subquery)
                conditions.append(f"({condition})")
            return " AND ".join(conditions)
        elif "OR" in query:
            for subquery in query["OR"]:
                condition = parse_query(subquery)
                conditions.append(f"({condition})")
            return " OR ".join(conditions)
    else:
        conditions = []
        for key, value in query.items():
            condition = build_condition(key, value)
            conditions.append(condition)
        return " AND ".join(conditions)


class Collection:
    """
    a collection within the apagea store
    """

    def __init__(self, name, db: str):
        self.name = name
        self.db = Database(f"sqlite+aiosqlite:///{db}.db")

    async def conn(self):
        await self.db.connect()
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA synchronous=NORMAL;")
        await self.db.execute("PRAGMA cache_size=10000;")
        await self.db.execute("PRAGMA foreign_keys=ON;")
        await self.db.execute("PRAGMA busy_timeout=5000;")
        return self

    async def validate(self):
        """
        validate the collection
        """
        pass

    async def count(self) -> int:
        """
        returns the number of records in the collection
        """
        conn = await self.conn()
        query = f"SELECT COUNT(*) as num_records FROM `col_{self.name}`;"
        count = await conn.db.fetch_one(query)
        return count["num_records"]

    async def insert(self, pk: str, data: dict):
        """
        creates a new record in the collection
        """
        conn = await self.conn()
        query = f"INSERT INTO `col_{self.name}` (pk, data) VALUES (:pk, :data);"
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}
        await conn.db.execute(query, values)

    async def insert_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        creates multiple records in the collection
        """
        conn = await self.conn()
        query = f"INSERT INTO `col_{self.name}` (pk, data) VALUES (:pk, :data);"
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await conn.db.execute_many(query, values)

    async def upsert(self, pk: str, data: dict):
        """
        will insert a new record if it does not exist,
        otherwise it will update the existing record
        """
        conn = await self.conn()
        query = (
            f"INSERT OR REPLACE INTO `col_{self.name}` (pk, data) VALUES (:pk, :data);"
        )
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}
        await conn.db.execute(query, values)

    async def upsert_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        will insert multiple records if they do not exist,
        otherwise it will update the existing records
        """
        conn = await self.conn()
        query = (
            f"INSERT OR REPLACE INTO `col_{self.name}` (pk, data) VALUES (:pk, :data);"
        )
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await conn.db.execute_many(query, values)

    async def inspect(self, pk: str) -> dict:
        """
        retrieves metadata about a record in the collection by primary key
        """
        pass

    async def get(self, pk: str, include_pk: bool = False) -> dict:
        """
        retrieves a record from the collection by primary key
        """
        conn = await self.conn()
        query = f"SELECT * FROM `col_{self.name}` WHERE pk = :pk;"
        values = {"pk": pk}
        record = await conn.db.fetch_one(query, values)
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
        conn = await self.conn()
        placeholders = ", ".join([f":pk{i}" for i in range(len(pks))])
        query = f"SELECT * FROM `col_{self.name}` WHERE pk IN ({placeholders});"
        values = {f"pk{i}": pk for i, pk in enumerate(pks)}

        try:
            records = await conn.db.fetch_all(query, values)
            if not include_pk:
                return [loads(record["data"]) for record in records]
            return {record["pk"]: loads(record["data"]) for record in records}

        except Exception as e:
            raise RuntimeError(f"Failed to fetch records: {e}")

    async def update(self, pk: str, data: dict):
        """
        Updates an existing record in the collection.
        """
        conn = await self.conn()
        query = f"UPDATE `col_{self.name}` SET data = :data WHERE pk = :pk;"
        datastr = dumps(data)
        values = {"pk": pk, "data": datastr}

        try:
            await conn.db.execute(query, values)
        except Exception as e:
            raise RuntimeError(f"Failed to update record: {e}")

    async def update_many(self, data: typing.List[typing.Tuple[str, dict]]):
        """
        updates multiple existing records in the collection
        """
        conn = await self.conn()
        query = f"UPDATE `col_{self.name}` SET data = :data WHERE pk = :pk;"
        values = [{"pk": pk, "data": dumps(data)} for pk, data in data]
        await conn.db.execute_many(query, values)

    async def delete(self, pk: str):
        """
        deletes a record from the collection by primary key
        """
        conn = await self.conn()
        query = f"DELETE FROM `col_{self.name}` WHERE pk = :pk;"
        values = {"pk": pk}
        await conn.db.execute(query, values)

    async def delete_many(self, pks: typing.List[str]):
        """
        Deletes multiple records from the collection by primary key.
        """
        conn = await self.conn()
        placeholders = ", ".join([f":pk{i}" for i in range(len(pks))])
        query = f"DELETE FROM `col_{self.name}` WHERE pk IN ({placeholders});"
        values = {f"pk{i}": pk for i, pk in enumerate(pks)}

        try:
            await conn.db.execute(query, values)
        except Exception as e:
            raise RuntimeError(f"Failed to delete records: {e}")

    async def list(
        self, limit: int = 10, offset: int = 0, include_pk: bool = False
    ) -> typing.List[dict]:
        """
        lists all records in the collection, paginated.
        The pk is included in the dictionary as `_pk`.
        """
        conn = await self.conn()
        query = f"SELECT * FROM `col_{self.name}` LIMIT :limit OFFSET :offset;"
        values = {"limit": limit, "offset": offset}
        records = await conn.db.fetch_all(query, values)
        if not include_pk:
            return [loads(record["data"]) for record in records]
        return [{record["pk"]: loads(record["data"])} for record in records]

    async def find(
        self, query: dict, limit: int = 10, include_pk: bool = False
    ) -> typing.List[dict]:
        """
        Finds records in the collection that match the query.
        """
        conn = await self.conn()
        query_str = parse_query(query)
        sql_query = f"SELECT * FROM `col_{self.name}` WHERE {query_str} LIMIT {limit};"

        try:
            records = await conn.db.fetch_all(sql_query)
            if not include_pk:
                return [loads(record["data"]) for record in records]
            return [{"pk": record["pk"], **loads(record["data"])} for record in records]
        except Exception as e:
            raise RuntimeError(f"Failed to find records: {e}")


class View:
    """
    A view within the apagea store.
    """

    def __init__(self, name: str, db: str):
        self.name = name
        self.db = Database(f"sqlite+aiosqlite:///{db}.db")

    async def conn(self):
        if not self.db.is_connected:
            await self.db.connect()
            await self.db.execute("PRAGMA journal_mode=WAL;")
            await self.db.execute("PRAGMA synchronous=NORMAL;")
            await self.db.execute("PRAGMA cache_size=10000;")
            await self.db.execute("PRAGMA foreign_keys=ON;")
            await self.db.execute("PRAGMA busy_timeout=5000;")
        return self

    async def count(self) -> int:
        """
        Returns the number of records in the view.
        """
        conn = await self.conn()
        query = f"SELECT COUNT(*) as num_records FROM `{self.name}`;"
        count = await conn.db.fetch_one(query)
        return count["num_records"]

    async def list(self, limit: int = 10, offset: int = 0):
        """
        Lists all records in the view.
        """
        conn = await self.conn()
        query = f"SELECT * FROM `{self.name}` LIMIT :limit OFFSET :offset;"
        values = {"limit": limit, "offset": offset}
        records = await conn.db.fetch_all(query, values)
        return [record for record in records]

    async def find(self, query: dict, limit: int = 10):
        """
        Finds records in the view that match the query.
        """
        conn = await self.conn()
        query_str = parse_query(query)
        sql_query = f"SELECT * FROM `{self.name}` WHERE {query_str} LIMIT {limit};"
        records = await conn.db.fetch_all(sql_query)
        return [record for record in records]

    async def delete(self):
        """
        Removes the view from the store.
        """
        conn = await self.conn()
        await conn.db.execute(f"DROP VIEW IF EXISTS `{self.name}`;")

    async def rename(self, new_name: str):
        """
        Renames the view.
        """
        conn = await self.conn()
        # Fetch the current definition of the view
        view_definition = await conn.db.fetch_one(
            f"SELECT sql FROM sqlite_master WHERE type='view' AND name='{self.name}';"
        )
        if not view_definition:
            raise RuntimeError(f"View '{self.name}' does not exist.")

        # Extract the view creation SQL
        create_view_sql = view_definition["sql"]
        # Remove the old view first
        await conn.db.execute(f"DROP VIEW `{self.name}`;")

        # Modify the view definition to use the new name and create the new view
        create_view_sql = create_view_sql.replace(
            f"CREATE VIEW `{self.name}`", f"CREATE VIEW `{new_name}`"
        )
        await conn.db.execute(create_view_sql)

        # Update the instance's name attribute
        self.name = new_name

    async def update(self, query: dict):
        """
        Updates the view with a new query.
        """
        conn = await self.conn()
        query_str = parse_query(query)
        await conn.db.execute(
            f"CREATE VIEW `{self.name}` AS SELECT * FROM `{self.name}` WHERE {query_str};"
        )


class Store:
    """
    a lightewight async storage utility that uses SQLite as it's backend
    """

    def __init__(self, name):
        self.name = name
        self.db = Database(f"sqlite+aiosqlite:///{name}.db")

    async def conn(self):
        # async with aiosqlite.connect(dsn) as db:
        await self.db.connect()
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA synchronous=NORMAL;")
        await self.db.execute("PRAGMA cache_size=10000;")
        await self.db.execute("PRAGMA foreign_keys=ON;")
        await self.db.execute("PRAGMA busy_timeout=5000;")
        return self

    async def __list_tables_in_db(self):
        """
        lists all tables in the database
        """
        conn = await self.conn()
        tables = await conn.db.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        return [table["name"] for table in tables]

    async def create_collection(self, collection_name: str) -> Collection:
        """
        creates a collection to store data within the apagea store
        """
        conn = await self.conn()

        # ensure the collection name is between 2 and 16 characters
        assert (
            len(collection_name) < 16 and len(collection_name) > 2
        ), "collection name must be between 2 and 16 characters"

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
        CREATE TABLE IF NOT EXISTS `col_{collection_name}` (
        `pk` TEXT PRIMARY KEY NOT NULL,
        `data` TEXT CHECK (json_valid(`data`)),
        `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""
        await conn.db.execute(create_collection_query)
        return Collection(name=collection_name, db=self.name)

    async def delete_collection(self, collection_name: str):
        """
        removes a collection from the apagea store, and all data within it.
        """
        conn = await self.conn()
        drop_collection_query = f"DROP TABLE IF EXISTS `col_{collection_name}`;"
        await conn.db.execute(drop_collection_query)

    async def list_collections(self) -> typing.List[Collection]:
        """
        lists all collections in the apagea store
        """
        _tables = await self.__list_tables_in_db()
        return [
            Collection(name=table.replace(COLLECTION_PREFIX, ""))
            for table in _tables
            if table.sw(COLLECTION_PREFIX)
        ]

    async def rename_collection(self, old_name: str, new_name: str):
        """
        renames a collection in the apagea store
        """
        conn = await self.conn()
        await conn.db.execute(
            f"ALTER TABLE `col_{old_name}` RENAME TO `col_{new_name}`;"
        )

    async def get_collection(
        self, collection_name: str, auto_create: bool = False
    ) -> Collection:
        """
        retrieves a collection by name
        """
        _tables = await self.__list_tables_in_db()
        if f"col_{collection_name}" not in _tables:
            if auto_create:
                return await self.create_collection(collection_name)
            raise CollectionNotFound(
                f"collection with name {collection_name} not found"
            )
        return Collection(name=collection_name, db=self.name)

    async def create_view(
        self,
        view_name: str,
        collection_name: str,
        fields: typing.List[str],
        query: typing.Optional[dict] = None,
    ) -> View:
        """
        Creates a view within the apagea store with the specified fields and conditions.
        """
        conn = await self.conn()

        # Constructing the SELECT statement with JSON extraction
        select_fields = ", ".join(
            [f"json_extract(data, '$.{field}') AS {field}" for field in fields]
        )
        view_query = f"""
        CREATE VIEW IF NOT EXISTS {view_name} AS
        SELECT pk, {select_fields}
        FROM `col_{collection_name}`
        """
        if query is not None:
            condition_str = parse_query(query)
            view_query += f" WHERE {condition_str};"

        try:
            await conn.db.execute(view_query)
        except Exception as e:
            raise RuntimeError(f"Failed to create view '{view_name}': {e}")

    async def delete_view(self, view_name: str):
        """
        removes a view from the apagea store
        """
        conn = await self.conn()
        await conn.db.execute(f"DROP VIEW IF EXISTS {view_name};")

    async def list_views(self) -> typing.List[View]:
        """
        lists all views in the apagea store
        """
        conn = await self.conn()
        views = await conn.db.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='view';"
        )
        return [View(name=view["name"]) for view in views]

    async def get_view(self, view_name: str) -> View:
        """
        retrieves a view by name
        """
        conn = await self.conn()
        view = await conn.db.fetch_one(
            f"SELECT name FROM sqlite_master WHERE type='view' AND name='{view_name}';"
        )
        if view is None:
            raise ViewNotFound(f"view with name {view_name} not found")
        return View(name=view["name"], db=self.name)

    async def destroy(self):
        """destroys the store, and all data within it"""
        assert Path(f"{self.name}.db").exists(), "store does not exist"
        Path(f"{self.name}.db").unlink()
