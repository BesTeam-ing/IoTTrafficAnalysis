from kafka import KafkaProducer
from json import dumps
from time import sleep
import uuid
import random
from datetime import datetime

def getCoordinates(routeId):
    if routeId == "Route-37":
        latPrefix = 33
        longPrefix = -96
    elif routeId == "Route-82":
        latPrefix = 34
        longPrefix = -97
    elif routeId == "Route-43":
        latPrefix = 35
        longPrefix = -98
    
    lati = int(latPrefix) + float("{:.6f}".format(random.random()))
    longi = int(longPrefix) + float("{:.6f}".format(random.random()))

    return lati, longi


def generateIoTEvent(producer, topic):
    routeList = ["Route-37", "Route-43", "Route-82"]
    vehicleTypeList = ["Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"]
    print("Sending events")
    
    while True:
        eventList = []
        for i in range(0, 100):
            vehicleId = str(uuid.uuid4())
            vehicleType = random.choice(vehicleTypeList)
            routeId = random.choice(routeList)
            timestamp = datetime.now()
            speed = random.randint(0, 80) + 20
            fuelLevel = random.randint(0, 30) + 10
            for j in range(0, 5):
                latitude, longitude = getCoordinates(routeId)
                event = {"vehicleId": vehicleId, "vehicleType": vehicleType, "routeId": routeId, "latitude": str(latitude), "longitude": str(longitude), "timestamp": str(timestamp), "speed": str(speed), "fuelLevel": str(fuelLevel)}
                eventList.append(event)

        random.shuffle(eventList)

        for event in eventList:
            print(event)
            producer.send(topic, value=event)
            sleep(random.randint(0,3) + 1)

if __name__ == "__main__":
    brokers = "localhost:9092"
    topic = "iot-data-event"
    zk = "localhost:2181"

    producer = KafkaProducer(bootstrap_servers=[brokers],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    generateIoTEvent(producer, topic)
