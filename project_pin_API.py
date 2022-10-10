import json
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()

#def serializer(data):
 #   return json.dumps(data).encode("utf-8")

#producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=serializer)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii"))

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send(topic="MyFirstKafkaTopic", value=data)
    #producer.send(topic = "MyFirstKafkaTopic", value = data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
