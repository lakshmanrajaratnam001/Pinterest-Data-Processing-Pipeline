Pinterest has world-class machine learning engineering systems. They have billions of user interactions such as image uploads or image clicks which they need to process every day to inform the decisions to make. In this project, I have built a system in the cloud that takes in those events and runs them through two separate pipelines. One for computing real-time metrics (like profile popularity, which would be used to recommend that profile in real-time), and another for computing metrics that depend on historical data (such as the most popular category this year).

Milestone 1: Data Ingestion-Configuring the API

I downloadad a zip file that contained an API (project_pin_API.py) which listened for events made by users on the app. The file also contained a user emulations script (user_posting_emulation.py) which simulates users uploading data to Pinterest.
 
First I ran the project_pin_API.py script to start the API and listened for POST requests on http://localhost:8000/pin/. Secondly, I ran the user_posting_emulation.py script to start simulating the user making POST requests to Pinterest. The API was receiving an 'OK' status for each POST request and was displayed in the terminal, confirming that data was being received successfully. 

Milestone 2: Data-Ingestion- Consuming the data in Kafka 

A Kafka topic was created to which the data would be sent to. The terminal was used to describe the topic to check it was created successfully. A KafkaProducer was created to send events from the API to Kafka. The 'get_db_row' method was then edited by specifying the topic name so data would be sent to the topic just created. Two Kafka-Consumer python files were then created, one for the batch processing pipeline called batch_consumer.py and one for the real time streaming pipeline called streaming_consumer.py. Within each file, the python-kafka library was used to make a consumer which recieved data from the topic created previously.

Milestone 3: Batch processing-Ingest data into the data lake

An AWS S3 bucket was created which will receive the data from the Kafka batch processing consumer. The bucket was named with a unique UIID4. Each event was saved as a JSON file. These files were set to the S3 bucket using the boto3 library for long term storage. 

Milestone 4: Batch processing- Processing the data using Spark

Areas where the data needed to be cleaned were identified, including replacing the 'k and M' with zeros for follower count, removing 'local save in' and dropping the index collumn. Collumns were also rearanged to display more important data first. A script was then written to have Spark read data from S3 bucket. A script was then written to clean the data previously specified. 

Milestone 5: Batch processing- Orchestrate the batch processing using Airflow. 

Milestone 6: Spark-straming 

Python file 'streaming_consumer.py' was created to consume data from kafka. Spark streaming jar file was submitted to the pyspark submit argument using the '--jars' option. Cleaning and feature computation transformations were carried out. 

Milestone 7: Streaming-Storage
Local Postgres database (pinterest_streaming) was curated with a schema that matched the attributes of the processed events. Database table was named 'experimental_data'. 'streaming_consumer.py' file was modified to have processed data sent to the database. 

