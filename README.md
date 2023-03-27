# kafka_coingecko_api 
-> working with public API and Apache Kafka

producer.ipynb is able to receive information about the trading volumes of all available exchanges in the form of a data stream from API 
and send them by the producer to the Kafka server;
consumer.ipynb is reading data by the consumer, which is then aggregated and accumulated in the form of a csv file.
