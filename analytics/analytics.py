from os import environ
from time import sleep,time
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, func, cast, select
from sqlalchemy.exc import OperationalError
from datetime import datetime, timedelta
import asyncio
import json
from math import acos, sin, cos


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

# Solution
while True:
    try:
        # creating MySQL engine
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')


# PSql devices table conn
metadata = MetaData()
metadata.reflect(bind=psql_engine)
devices = Table('devices', metadata, autoload=True, autoload_with=psql_engine)


# MySQL conn
metadata_obj = MetaData()
aggregated_results = Table(
    'aggregated_results', metadata_obj,
    Column('device_id', String(255)),
    Column('max_temperature', Float),
    Column('data_point_count', Integer),
    Column('total_distance', Float),
    Column('time', String(255)),
)
metadata_obj.create_all(mysql_engine)


#Calculating distance using latitudes and longitudes
def calculate_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    distance = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371
    return distance



# Calculating:
# Maximum temperatures measured for every device per hour.
# Amount of data points aggregated for every device per hour.
# Total distance of device movement for every device per hour.
async def aggregate_data():
    while True:
        # Calculate start and end time for the hourly interval
        end_time = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_time = end_time - timedelta(hours=1)
        print(f"Start time {start_time} and End time {end_time}")

        # Get all unique device IDs in the last hour
        devices_query = devices.select().where(cast(devices.c.time, Integer) >= int(start_time.timestamp()))
        device_ids = set(r['device_id'] for r in psql_engine.execute(devices_query))

        # Aggregate data for each device
        for device_id in device_ids:
            # Maximum temperature
            temp_agg = psql_engine.execute(
                select([func.max(devices.c.temperature)]).where(
                    devices.c.device_id == device_id and
                    devices.c.time >= int(start_time.timestamp()) and
                    devices.c.time < int(end_time.timestamp())
                )
            ).scalar()

            # Data points
            data_points_agg = psql_engine.execute(
                select([func.count()]).where(
                    devices.c.device_id == device_id and
                    devices.c.time >= int(start_time.timestamp()) and
                    devices.c.time < int(end_time.timestamp())
                )
            ).scalar()

            # Total distance
            locations_query = devices.select().where(
                devices.c.device_id == device_id and
                devices.c.time >= int(start_time.timestamp()) and
                devices.c.time < int(end_time.timestamp())
            )
            locations = [json.loads(r['location']) for r in psql_engine.execute(locations_query)]
            distance = 0.0
            for i in range(1, len(locations)):
                lat1, lon1 = float(locations[i-1]['latitude']), float(locations[i-1]['longitude'])
                lat2, lon2 = float(locations[i]['latitude']), float(locations[i]['longitude'])
                distance += calculate_distance(lat1, lon1, lat2, lon2)

            # Store aggregated data in MySQL
            mysql_engine.execute(
                aggregated_results.insert().values(
                    device_id=device_id,
                    max_temperature=temp_agg,
                    data_point_count=data_points_agg,
                    total_distance=distance,
                    time=start_time  # Converting EPoch time to datetime type for MySQL
                )
            )
        print(f"Aggregated data between Start time {start_time} and End time {end_time} ingested successfully")

        # Wait for an hour to perform the next aggregation
        print("Waiting for one hour to get data accumulated")
        await asyncio.sleep(3600)


loop = asyncio.get_event_loop()
asyncio.ensure_future(
    aggregate_data()
)

loop.run_forever()
