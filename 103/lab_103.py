import httpx  # requests capability, but can work with async
from prefect import flow, task, get_run_logger


@flow
def fetch1(lat: float, lon: float):
    fetch_weather(lat, lon)


@task(retries=2, retry_delay_seconds=0.1)
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    return most_recent_temp


@flow
def fetch2(lat: float, lon: float):
    fetch_weather_wind(lat, lon)


@task(retries=2, retry_delay_seconds=0.1)
def fetch_weather_wind(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="windspeed_10m"),
    )
    most_recent_wind_speed = float(weather.json()["hourly"]["windspeed_10m"][0])
    return most_recent_wind_speed



@flow(name="subflow-logger")
def log_it():
    logger = get_run_logger()
    logger.info("INFO level. Logged with get_run_logger")


@flow(log_prints=True)
def pipeline(lat: float, lon: float):
    log_it()
    temp = fetch1(lat, lon)
    temp2 = fetch2(lat, lon)
    print(f"temperature: {temp} \n wind: {temp2}")


if __name__ == "__main__":
    pipeline(40.40, 14.47)
