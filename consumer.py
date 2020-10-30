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
import io
import logging
import os
import traceback

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
			"bootstrap.servers": f"{TOPIC}:{PORT}",
			"auto.offset.reset": "earliest",
			"enable.auto.commit": False,
			"max.poll.records": 1
		}
	)

	consumer.subscribe([TOPIC])

	while True:
		try:
			message = consumer.poll(timeout=10)

			if message is None:
				continue
			elif message.error():
				logger.error(f"Error... \n {message.error()}")
			else:
				print(message)
				print(message.value())

		except KeyboardInterrupt:
			logger.info("\n")
			logger.info("Received request to end subscribe")

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
# ----- Process Main
# **************************************************
if __name__ == "__main__":
	main()


# **************************************************
# ----- End
# **************************************************
