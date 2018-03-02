import sys
import json
import yaml
import os
import time
import datetime
import random
from kafka import KafkaProducer, KeyedProducer

ticker_list_path = "./companylist.csv"
ipfile_path = 'ip_addresses.txt'

def create_stock_list(filename):
    """ Open a csv file that lists up all the stock ticker symbols
        Args:   <filename: string>
        Return: ticker_list: list
    """         
    myfile = open(filename, "r")
    ticker_list = []
    count = 0

    for line in myfile:
        tickers = line.strip().split(',')
        ticker_list.append(tickers[0])
        
    myfile.close()    
    return ticker_list


def main():
    """ simulate stock price information as json to Kafka
        with optional delay between messages
    """

    if len(sys.argv) < 2:
      print "Usage: ./kafka_producer_stock.py stock-topic"
      sys.exit(1)

    if len(sys.argv) >= 3:
      wait_time = float(sys.argv[2])
    else:
      wait_time = 0

    # Set up kafka brokers
    ipfile = open(ipfile_path, 'r')
    ips = ipfile.read()[:-1]
    ipfile.close()
    ips = ips.split('\n')

    producer_user = (KafkaProducer(bootstrap_servers=ips, 
                value_serializer=lambda v: json.dumps(v).encode('utf-8')))

    ticker_list = create_stock_list(ticker_list_path)

    # simulate data 
    while True:
      cur_time = datetime.datetime.now()
      time_string = str(cur_time.hour).zfill(2) + ":" + str(cur_time.minute).zfill(2) \
      + ":" + str(cur_time.second).zfill(2)
      for ticker_name in ticker_list:
        stock_price = float(random.randint(20, 20000)/10)
        stock_record = {"ticker": ticker_name, "price": stock_price, "time": time_string}
   
        # send the messages to separate topics
        producer_user.send(sys.argv[1], stock_record) 
      time.sleep(wait_time)     

if __name__ == "__main__":
    main()