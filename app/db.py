import os
import asyncpg
import redis.asyncio as aioredis

from aiohttp import web


async def create_table_if_not_exists(app: web.Application) -> None:
    async with app["pg_pool"].acquire() as connection:
        await connection.execute(
            """CREATE TABLE IF NOT EXISTS cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL
            )"""
        )

        print("Table checked/created successfully.")


async def create_db_pool(app: web.Application) -> None:
    try:
        app["pg_pool"] = await asyncpg.create_pool(
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            database=os.environ["POSTGRES_DB"],
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["PGPORT"],
        )

        await create_table_if_not_exists(app)  # Проверяем и создаем таблицу
    except Exception as e:
        print(f"Error creating postgres pool: {e}")
        raise


async def close_db_pool(app: web.Application) -> None:
    if "pg_pool" in app:
        await app["pg_pool"].close()


async def create_redis_pool(app: web.Application) -> None:
    try:
        app["redis"] = await aioredis.from_url(f'redis://{os.environ["REDIS_HOST"]}')
    except TimeoutError as e:
        print(f"Error connecting to Redis1: {e}")
        raise
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        raise


async def close_redis_pool(app: web.Application) -> None:
    if "redis" in app:
        await app["redis"].close()
