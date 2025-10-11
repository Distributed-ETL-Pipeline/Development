import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2

# GTFS Static URL for MARTA
GTFS_URL = "https://itsmarta.com/google_transit_feed/google_transit.zip"

# Use HTML get to retrieve the static GTFS data
response = requests.get(GTFS_URL)
if response.status_code != 200:
    raise Exception(f"Failed to download GTFS feed. Status code: {response.status_code}")


with zipfile.ZipFile(io.BytesIO(response.content)) as staticData:
    #List available GTFS files
    print("Files in GTFS feed:")
    print(staticData.namelist())

    # Step 3: Read specific files into DataFrames
    if "stops.txt" in staticData.namelist():
        stops = pd.read_csv(staticData.open("stops.txt"))
       
    if "routes.txt" in staticData.namelist():
        routes = pd.read_csv(staticData.open("routes.txt"))
   
    if "trips.txt" in staticData.namelist():
         trips = pd.read_csv(staticData.open("trips.txt"))

    if "agency.txt" in staticData.namelist():
         agency = pd.read_csv(staticData.open("agency.txt"))

    if "calendar.txt" in staticData.namelist():
         calendar = pd.read_csv(staticData.open("calendar.txt"))

    if "shapes.txt" in staticData.namelist():
         shapes = pd.read_csv(staticData.open("shapes.txt"))

    if "calendar_dates.txt" in staticData.namelist():
         calendar_dates = pd.read_csv(staticData.open("calendar_dates.txt"))

    if "stop_times.txt" in staticData.namelist():
         stop_times = pd.read_csv(staticData.open("stop_times.txt"))


stops.to_csv("stops.csv", index=False)
trips.to_csv("trips.csv", index=False)
routes.to_csv("routes.csv", index=False)
stop_times.to_csv("stop_times.csv", index=False)


#MARTA GTFS Realtime vehicle position
VEHICLES_URL = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/vehicle/vehiclepositions.pb"

#MARTA GTFS Realtime trip updates
TRIPS_URL = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/tripupdate/tripupdates.pb"

#Download the .pb vehicle feed
response_vehicles = requests.get(VEHICLES_URL)
response_vehicles.raise_for_status()

#Parse with gtfs-realtime-bindings
feed_vehicles = gtfs_realtime_pb2.FeedMessage()
feed_vehicles.ParseFromString(response_vehicles.content)

#Convert to a list of dicts
records_vehicles = []
for entity in feed_vehicles.entity[:]:
    if entity.HasField("vehicle"):
        vehicle = entity.vehicle
        records_vehicles.append({
            "vehicle_id": vehicle.vehicle.id,
            "trip_id": vehicle.trip.trip_id,
            "route_id": vehicle.trip.route_id,
            "lat": vehicle.position.latitude,
            "lon": vehicle.position.longitude,
            "bearing": vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
            "speed": vehicle.position.speed if vehicle.position.HasField("speed") else None,
            "timestamp": pd.to_datetime(vehicle.timestamp, unit="s", utc=True),
        })

#Load into Pandas DataFrame
df_v = pd.DataFrame(records_vehicles)
pd.set_option('display.max_columns', None)

#Save DataFrame as .csv file
df_v.to_csv("MartaVehicleData.csv", index=False)

#Download the .pb trip feed
response_trips = requests.get(TRIPS_URL)
response_trips.raise_for_status()

#Parse with gtfs-realtime-bindings
feed_trips = gtfs_realtime_pb2.FeedMessage()
feed_trips.ParseFromString(response_trips.content)


# Convert GTFS-Realtime trip updates to a list of dicts
records_trips = []

for entity in feed_trips.entity[:]:
    if entity.HasField("trip_update"):
        trip_update = entity.trip_update
        trip = trip_update.trip

        for stop_update in trip_update.stop_time_update:
            records_trips.append({
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "stop_id": stop_update.stop_id,
                "stop_sequence": (
                    stop_update.stop_sequence if stop_update.HasField("stop_sequence") else None
                ),
                "arrival_time": (
                    pd.to_datetime(stop_update.arrival.time, unit="s", utc=True)
                    if stop_update.HasField("arrival") and stop_update.arrival.HasField("time")
                    else None
                ),
                "departure_time": (
                    pd.to_datetime(stop_update.departure.time, unit="s", utc=True)
                    if stop_update.HasField("departure") and stop_update.departure.HasField("time")
                    else None
                ),
                "arrival_delay": (
                    stop_update.arrival.delay
                    if stop_update.HasField("arrival") and stop_update.arrival.HasField("delay")
                    else None
                ),
                "departure_delay": (
                    stop_update.departure.delay
                    if stop_update.HasField("departure") and stop_update.departure.HasField("delay")
                    else None
                ),
                "timestamp": (
                    pd.to_datetime(trip_update.timestamp, unit="s", utc=True)
                    if trip_update.HasField("timestamp")
                    else None
                ),
            })


            
#Load into Pandas DataFrame
df_t = pd.DataFrame(records_trips)
pd.set_option('display.max_columns', None)

#Save DataFrame as .csv file
df_t.to_csv("MartaTripData.csv", index=False)
