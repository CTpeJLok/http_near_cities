import json

from aiohttp import web

import asyncpg
import redis.asyncio as aioredis

from errors import NoCityException, GetCoordinatesException
from db import create_db_pool, close_db_pool, create_redis_pool, close_redis_pool
from distance import haversine, get_coordinates


async def get_all(request) -> web.Response:
    """ """

    try:
        pg_pool: asyncpg.pool.Pool = request.app["pg_pool"]

        async with pg_pool.acquire() as connection:
            result = await connection.fetch(f"SELECT * FROM cities")
            data = [dict(record) for record in result]

            return web.json_response({"cities": data})
    except asyncpg.PostgresError as e:
        return web.json_response({"error": str(e)}, status=500)
    except Exception:
        return web.json_response({"error": "An unexpected error occurred"}, status=500)


async def get(request) -> web.Response:
    """
    Возвращает координаты города. Если город не найден, то возвращается код состояния 404.

    Пример запроса: /get/1

    Пример ответа:
    \{
        "id": 1,
        "name": "Moscow",
        "lat": 55.7522,
        "lon": 37.6156
    }
    """

    try:
        pg_pool: asyncpg.pool.Pool = request.app["pg_pool"]

        # Получение параметра id из URL-адреса запроса
        id = int(request.match_info["id"])

        async with pg_pool.acquire() as connection:
            result = await connection.fetchrow(f"SELECT * FROM cities WHERE id = {id}")

            if result:
                result = dict(result)
                return web.json_response(
                    {
                        "id": result["id"],
                        "name": result["name"],
                        "lat": result["lat"],
                        "lon": result["lon"],
                    }
                )

            return web.json_response(status=404)
    except asyncpg.PostgresError as e:
        return web.json_response({"error": str(e)}, status=500)
    except Exception:
        return web.json_response({"error": "An unexpected error occurred"}, status=500)


async def create(request) -> web.Response:
    """
    Добавляет город в базу данных. В ответе возвращается его координаты. Если город уже существует, то он не добавляется.

    Пример запроса:
    \{
        "name": "Moscow"
    }

    Пример ответа:
    \{
        "id": 1,
        "name": "Moscow",
        "lat": 55.7522,
        "lon": 37.6156
    }
    """

    try:
        pg_pool: asyncpg.pool.Pool = request.app["pg_pool"]
        redis: aioredis.Redis = request.app["redis"]

        # Получение JSON-данных из тела запроса
        data = await request.json()
        name = data.get("name")

        async with pg_pool.acquire() as connection:
            result = await connection.fetchrow(
                f"SELECT * FROM cities WHERE name = '{name}'"
            )

            # Если город уже существует, то возвращаем его координаты
            if result:
                result = dict(result)
                return web.json_response(
                    {
                        "id": result["id"],
                        "name": result["name"],
                        "lat": result["lat"],
                        "lon": result["lon"],
                    }
                )

            # Запрос координат для города из API
            lat, lon = await get_coordinates(name)

            # Вставка данных в БД
            id = await connection.fetchval(
                "INSERT INTO cities (name, lat, lon) VALUES ($1, $2, $3) RETURNING id;",
                name,
                lat,
                lon,
            )

            if not id:
                raise Exception("Failed to insert city into database")

            # Очистка кэша
            await redis.flushall()

            return web.json_response(
                {
                    "id": id,
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                }
            )
    except asyncpg.PostgresError as e:
        return web.json_response({"error": str(e)}, status=500)
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except NoCityException as e:
        return web.json_response({"error": str(e)}, status=400)
    except GetCoordinatesException as e:
        return web.json_response({"error": str(e)}, status=400)
    except Exception as e:
        return web.json_response(
            {"error": f"An unexpected error occurred\n{e}"}, status=500
        )


async def delete(request) -> web.Response:
    """
    Удаляет город из базы данных.

    Пример запроса: /delete/1
    """

    try:
        pg_pool: asyncpg.pool.Pool = request.app["pg_pool"]
        redis: aioredis.Redis = request.app["redis"]

        # Получение параметра id из URL-адреса запроса
        id = int(request.match_info["id"])

        async with pg_pool.acquire() as connection:
            await connection.execute(f"DELETE FROM cities WHERE id = {id}")

            # Очистка кэша
            await redis.flushall()

            return web.json_response()
    except asyncpg.PostgresError as e:
        return web.json_response({"error": str(e)}, status=500)
    except Exception as e:
        return web.json_response(
            {"error": f"An unexpected error occurred\n{e}"}, status=500
        )


async def get_near_cities(request) -> web.Response:
    try:
        redis: aioredis.Redis = request.app["redis"]

        # Получение JSON-данных из тела запроса
        data = await request.json()
        lat = float(data.get("lat"))
        lon = float(data.get("lon"))

        cache_key = f"{lat},{lon}"
        cached_data = await redis.get(cache_key)
        if cached_data:
            return web.json_response(json.loads(cached_data))

        pg_pool: asyncpg.pool.Pool = request.app["pg_pool"]
        async with pg_pool.acquire() as connection:
            result = await connection.fetch("SELECT * FROM cities")
            data = [dict(record) for record in result]

            distances: list[tuple[str, float]] = [
                (city["name"], haversine(lat, lon, city["lat"], city["lon"]))
                for city in data
            ]
            sorted_distances: list[tuple[str, float]] = sorted(
                distances, key=lambda x: x[1]
            )[:2]
            final_result: dict[str, list[str]] = {
                "result": [i[0] for i in sorted_distances]
            }

            await redis.set(cache_key, json.dumps(final_result), ex=60 * 60 * 24)

            return web.json_response(final_result)

    except asyncpg.PostgresError as e:
        return web.json_response({"error": str(e)}, status=500)
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception:
        return web.json_response({"error": "An unexpected error occurred"}, status=500)


app = web.Application()
app.add_routes(
    [
        web.get("/get", get_all),
        web.get("/get/{id}", get),
        web.post("/create", create),
        web.delete("/delete/{id}", delete),
        web.post("/near", get_near_cities),
    ]
)

app.on_startup.append(create_db_pool)
app.on_startup.append(create_redis_pool)

app.on_cleanup.append(close_db_pool)
app.on_cleanup.append(close_redis_pool)

if __name__ == "__main__":
    web.run_app(app, port=8000)
