import pytest
import random
import randomname
from pathlib import Path
from documentdb import Store


@pytest.mark.asyncio
async def test_create_store():
    s = Store("example")
    await s.conn()
    assert s.name == "example"
    assert Path("example.db").exists()
    await s.destroy()


@pytest.mark.asyncio
async def test_create_collection():
    s = Store("example")
    c = await s.create_collection("animals")
    assert c.name == "animals"
    await s.destroy()


@pytest.mark.asyncio
async def test_insert_into_collection():
    s = Store("example")
    animals = await s.create_collection("animals")

    await animals.insert(
        "blueberry", {"name": "Blueberry", "breed": "Malteese", "age": 4}
    )
    await animals.insert("luna", {"name": "Luna", "breed": "Corgi", "age": 2})
    assert await animals.count() == 2
    await s.destroy()


@pytest.mark.asyncio
async def test_insert_many_into_collection():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)
    assert await animals.count() == 1000
    await s.destroy()


@pytest.mark.asyncio
async def test_get_single_record():
    s = Store("example")
    animals = await s.create_collection("animals")

    await animals.insert(
        "blueberry", {"name": "Blueberry", "breed": "Malteese", "age": 4}
    )
    await animals.insert("luna", {"name": "Luna", "breed": "Corgi", "age": 2})

    record = await animals.get("luna")
    assert record["name"] == "Luna"
    assert record["breed"] == "Corgi"
    assert record["age"] == 2
    await s.destroy()


@pytest.mark.asyncio
async def test_get_many_records():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)

    records = await animals.get_many(
        ["dog-0", "dog-123", "dog-234", "dog-345", "dog-999"]
    )
    assert len(records) == 5
    assert records[0]["name"] == "dog-0"
    assert records[1]["name"] == "dog-123"
    assert records[2]["name"] == "dog-234"
    assert records[3]["name"] == "dog-345"
    assert records[4]["name"] == "dog-999"
    await s.destroy()


@pytest.mark.asyncio
async def test_update_record():
    s = Store("example")
    animals = await s.create_collection("animals")

    await animals.insert(
        "blueberry", {"name": "Blueberry", "breed": "Malteese", "age": 4}
    )

    await animals.update("blueberry", {"age": 5})
    record = await animals.get("blueberry")
    assert record["age"] == 5
    await s.destroy()


@pytest.mark.asyncio
async def test_update_many():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)

    new_records = []
    for i in range(1000):
        new_records.append([f"dog-{i}", {"size": "small"}])
    await animals.update_many(new_records)
    d0 = await animals.get("dog-0")
    assert d0["size"] == "small"
    d999 = await animals.get("dog-999")
    assert d999["size"] == "small"
    await s.destroy()


@pytest.mark.asyncio
async def test_delete_record():
    s = Store("example")
    animals = await s.create_collection("animals")

    await animals.insert(
        "blueberry", {"name": "Blueberry", "breed": "Malteese", "age": 4}
    )

    await animals.delete("blueberry")
    assert await animals.count() == 0
    await s.destroy()


@pytest.mark.asyncio
async def test_delete_many_records():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)

    await animals.delete_many(["dog-0", "dog-1", "dog-2"])
    assert await animals.count() == 997
    await s.destroy()


@pytest.mark.asyncio
async def test_list_records():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)

    records = await animals.list()
    assert len(records) == 10
    await s.destroy()


@pytest.mark.asyncio
async def test_pagination_of_records():
    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    for i in range(1000):
        records.append([f"dog-{i}", {"name": f"dog-{i}", "legs": 4}])
    await animals.insert_many(records)

    records = await animals.list()
    assert len(records) == 10
    assert records[0]["name"] == "dog-0"
    assert records[9]["name"] == "dog-9"

    records = await animals.list(offset=10, limit=20)
    assert len(records) == 20
    assert records[0]["name"] == "dog-10"
    assert records[19]["name"] == "dog-29"

    await s.destroy()


@pytest.mark.asyncio
async def test_find_records():
    # example query:
    # query = {
    #     "AND": [
    #         {"age": {"gt": 50}},
    #         {"OR": [
    #             {"name": {"startswith": "J"}},
    #             {"city": "New York"}
    #         ]}
    #     ]
    # }

    s = Store("example")
    animals = await s.create_collection("animals")

    records = []
    over_50 = []
    males = []
    females = []
    for i in range(100):
        name = randomname.get_name()
        age = random.randint(1, 100)
        sex = random.choice(["male", "female"])
        if age > 50:
            over_50.append(name)
        if sex == "male":
            males.append(name)
        else:
            females.append(name)
        records.append(
            [
                f"random-{i}",
                {
                    "name": name,
                    "age": age,
                    "sex": sex,
                },
            ]
        )
    await animals.insert_many(records)

    # find all the records where age is greater than 50
    query = {"age": {"gt": 50}}
    records = await animals.find(query, limit=100)
    assert len(records) == len(over_50)

    # find all the males
    query = {"sex": {"eq": "male"}}
    records = await animals.find(query, limit=100)
    assert len(records) == len(males)

    await s.destroy()
