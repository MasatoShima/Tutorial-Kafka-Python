"""
Name: consumer.py
Created by: Masato Shima
Created on: 2020/10/05
Description:
	Kafka consumer
"""

# **************************************************
# ----- Import Library
# **************************************************
import datetime
import json
import logging
import os
import traceback
from typing import Any, Dict

import fastavro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


# **************************************************
# ----- Constants & Variables
# **************************************************
HOST = "10.2.152.95"
PORT = "9092"
TOPIC = "SKDB.public.sdcocdmst"

DIR_OUTPUT = "data/output/"


# **************************************************
# ----- Set logger
# **************************************************
logger = logging.getLogger(str(os.path.basename(__file__).split(".")[0]))
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s %(name)-8s %(levelname)-8s %(message)s"))

logger.addHandler(handler)


# **************************************************
# ----- Function main
# **************************************************
def main() -> None:
	logger.info("Start process...")

	subscribe_message()

	logger.info("End process...")

	return


# **************************************************
# ----- Function subscribe_message
# **************************************************
def subscribe_message() -> None:
	logger.info("Start subscribe messages...")

	os.makedirs(DIR_OUTPUT, exist_ok=True)

	consumer = AvroConsumer(
		{
			"bootstrap.servers": f"{HOST}:{PORT}",
			"group.id": "test",
			"auto.offset.reset": "earliest",
			"enable.auto.commit": False,
			"schema.registry.url": f"http://{HOST}:8081"
		}
	)

	consumer.subscribe([TOPIC])

	schema = read_schema()

	while True:
		try:
			message = consumer.poll(timeout=10)

			if message is None:
				continue
			elif message.value() is None:
				continue
			elif message.error():
				logger.error(f"Error... \n {message.error()}")
			else:
				print(json.dumps(message.value(), ensure_ascii=False))
				write_message(message.value(), schema)

		except KeyboardInterrupt:
			logger.info("Received request to end subscribe")

			consumer.close()

			break

		except SerializerError as error:
			logger.error(
				f"SerializerError..."
				f"{error}"
				f"{traceback.format_exc()}"
			)

			break

		except Exception as error:
			logger.error(
				f"Unknown exception..."
				f"{error}"
				f"{traceback.format_exc()}"
			)

			break

	logger.info("End subscribe messages...")

	return


# **************************************************
# ----- Function read_schema
# **************************************************
def read_schema() -> fastavro.schema:
	with open(f"avro/schema/schema-{TOPIC}.json", "r") as file:
		schema = json.load(file)
		schema = json.loads(schema["schema"])
		schema = fastavro.parse_schema(schema)

	return schema


# **************************************************
# ----- Function write_message
# **************************************************
def write_message(message: Dict[str, Any], schema: fastavro.schema) -> None:
	filename = f"avro-{TOPIC}-{datetime.datetime.now().timestamp()}.avro"

	with open(f"avro/{filename}", "wb") as file:
		fastavro.writer(file, schema, [message])

	return


# **************************************************
# ----- Process Main
# **************************************************
if __name__ == "__main__":
	main()


# **************************************************
# ----- End
# **************************************************
