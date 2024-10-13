import math
import os

from aiohttp import ClientSession

from errors import NoCityException, GetCoordinatesException


# Функция для вычисления расстояния по формуле haversine
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Функция для вычисления расстояния между двумя точками на плоскости
    """

    R = 6371  # Радиус Земли в километрах
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


async def get_coordinates(city_name) -> tuple[float, float]:
    """
    Функция для получения координат города по его названию с использованием API Geoapify.
    """

    api_url = f"https://api.geoapify.com/v1/geocode/search?text={city_name}&apiKey={os.environ['API_KEY']}"

    async with ClientSession() as session:
        async with session.get(api_url) as response:
            if response.status == 200:
                try:
                    data = await response.json()

                    city = data.get("features")

                    if not city:
                        raise NoCityException(f"No city found for {city_name}")

                    city = city[0]
                    properties = city["properties"]
                    return properties["lat"], properties["lon"]
                except NoCityException as e:
                    raise e
                except Exception as e:
                    raise GetCoordinatesException(f"Error getting coordinates: {e}")
            else:
                raise GetCoordinatesException(
                    f"API request failed with status {response.status}"
                )
