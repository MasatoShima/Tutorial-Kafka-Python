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
import logging
import os

from kafka import KafkaConsumer


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

	try:
		consumer = KafkaConsumer(TOPIC)

		i = 0

		for message in consumer:
			print(message)

			file_name = f"message_{int(datetime.datetime.today().timestamp())}"

			with open(file_name, "wb") as file:
				file.write(message)

			if i <= 10:
				continue
			else:
				break

	except KeyboardInterrupt:
		logger.info("Received request to end subscribe")

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