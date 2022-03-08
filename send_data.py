import json
import random
import string
import time
from typing import Any, Dict

from faker import Faker
from google.cloud import pubsub_v1

project_id = "nttdata-c4e-bde"



publisher = pubsub_v1.PublisherClient()

input_topic = "projects/nttdata-c4e-bde/topics/uc1-input-topic-3"

fake = Faker()




def create_right_message_body(id :int) -> Dict[str, Any]:
    # create json and encode data
    gender = "M" if (random.randint(0, 1) == 1) else "F"
    # first name
    name = fake.first_name_male() if gender == "M" else fake.first_name_female()
    # last name
    surname = last_name = fake.last_name()
    record = {"id": id, "name": name, "surname": surname}
    return record


def create_wrong_message_body() -> str:
    letters = string.ascii_lowercase
    digits = string.digits
    error_record = "".join(
        random.choice(letters) + random.choice(digits) for i in range(10)
    )
    return error_record


for i in range(30):
    print(f"Publish message {i}th in Topic")

    # generate/ publish right and wrong messages
    error = "error" if (random.randint(0, 1) == 1) else "clean"
    if error == "clean":
        record = create_right_message_body(i)
        print("clean message: ", record)
        future = publisher.publish(input_topic, json.dumps(record).encode("utf-8"))
    else:
        record = create_wrong_message_body()
        print("error record: ", record)
        future = publisher.publish(input_topic, record.encode("utf-8"))

    # message id start from 1 <> from 0 as my for loop
    print(f"published message id {future.result()}")
    time.sleep(1)
