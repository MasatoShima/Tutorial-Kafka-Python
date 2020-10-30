"""
Name: avro_handler.py
Created by: Masato Shima
Created on: 2020/10/12
Description:
"""

# **************************************************
# ----- Import Library
# **************************************************
import json

import fastavro


# **************************************************
# ----- Constants & Variables
# **************************************************


# **************************************************
# ----- Set logger
# **************************************************


# **************************************************
# ----- Function main
# **************************************************
def main() -> None:
	# read_schema()

	read_avro()

	return


# **************************************************
# ----- Function read_schema
# **************************************************
def read_schema() -> fastavro.schema:
	with open("data/sample_1.json", "rb") as file:
		schema = json.load(file)
		schema = fastavro.parse_schema(schema["schema"]["fields"][0])

	return schema


# **************************************************
# ----- Function read_avro
# **************************************************
def read_avro():
	with open("data/sample.avro", "rb") as file:
		for record in fastavro.reader(file):
			print(record)

	return


# **************************************************
# ----- Process Main
# **************************************************
if __name__ == "__main__":

	main()


# **************************************************
# ----- End
# **************************************************
