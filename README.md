# kafka & coingecko api 
-> working with public API and Apache Kafka
1. Following the link: https://www.coingecko.com/en/api/documentation and looking for the API methods for obtaining information about the list of all exchanges and their trading volumes.
2. Reading the list of all exchanges from the API and getting information about the trading volumes for one last day for any of the exchanges.
3. Implementing a generator function that iterates through a list of all exchanges (which may be obtained from an API and saved in memory), retrieves the trading volume for each exchange for the last day, and creates a JSON object of the form {'exchange_name': volume_one_day_data}. This JSON can then be encoded for transmission to Kafka using the str.encode method. At the end of each iteration, the function should yield the encoded JSON as bytes.
4. Sending the data to a Kafka server in an ipynb notebook with a producer.
5. Implementing a function that:
- takes in data on trading volumes for a single exchange for one day;
- aggregates the data (using np.mean) into 4-hour windows (i.e. 24/4 = 6 trading volume values per day for each exchange);
- saves the intermediate result (e.g. in a list or pd.DataFrame), and for every five exchanges, saves the data to a CSV file with columns [exchange, volume_1, volume_2, ..., volume_6];
- if the file already exists and the first five/ten (and so on) exchanges are already recorded in it, then on every fifth exchange, the function should read the existing file, append new data to it, and save it again.

As the result, there are two file: 
1. producer.ipynb is able to receive information about the trading volumes of all available exchanges in the form of a data stream from API 
and send them by the producer to the Kafka server;
2. consumer.ipynb is reading data by the consumer, which is then aggregated and accumulated in the form of a csv file.
