"""
Name: producer.py
Created by: Masato Shima
Created on: 2020/10/01
Description:
	Kafka producer
"""

# **************************************************
# ----- Import Library
# **************************************************
import datetime
import json
import logging
import os

from kafka import KafkaProducer


# **************************************************
# ----- Constants & Variables
# **************************************************
HOST = "localhost"
PORT = "9092"

TOPIC = "quickstart-events"


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
def main():
	logger.info("Start process...")

	publish_message()

	logger.info("End process...")

	return


# **************************************************
# ----- Function publish_message
# **************************************************
def publish_message():
	logger.info("Start publish messages...")

	producer = KafkaProducer(bootstrap_servers=f"{HOST}:{PORT}")

	for _ in range(0, 10):
		value = {
			"time": str(datetime.datetime.now())
		}

		producer.send(topic=TOPIC, value=json.dumps(value))

	logger.info("End publish messages...")

	return


# **************************************************
# ----- Process Main
# **************************************************
if __name__ == "__main__":
	main()


# **************************************************
# ----- End
# **************************************************
