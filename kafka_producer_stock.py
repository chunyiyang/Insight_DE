import sys
import json
import yaml
import os
import time
import datetime
import random
from kafka import KafkaProducer, KeyedProducer

#ticker_list_path = "./ticker_name_list.csv"
ticker_list_path = "./companylist.csv"


def create_stock_list(filename):
    """ Open a csv file that lists up all the stock ticker symbols
    and converts to a list """
    
    myfile = open(filename, "r")
    ticker_list = []
    count = 0

    for line in myfile:
        tickers = line.strip().split(',')
        ticker_list.append(tickers[0])
        
    myfile.close()    
    return ticker_list



def main():
  """Reads input file and sends the lines as json to Kafka
     with optional delay between messages"""

  if len(sys.argv) < 2:
    print "Usage: ./kafka_producer_stock.py stock-topic"
    sys.exit(1)

  if len(sys.argv) >= 3:
    wait_time = float(sys.argv[2])
  else:
    wait_time = 0

  # Set up Producer: send to all 4 instances, encode json
  ipfile = open('ip_addresses.txt', 'r')
  ips = ipfile.read()[:-1]
  ipfile.close()
  ips = ips.split('\n')

  producer_user = (KafkaProducer(bootstrap_servers=ips, 
              value_serializer=lambda v: json.dumps(v).encode('utf-8')))

  cur_time = datetime.datetime.now()
  time_string = str(cur_time.year) + ":" + str(cur_time.month) + ":" + str(cur_time.day) + ":" + str(cur_time.minute) + ":" + str(cur_time.second)
  ticker_list = create_stock_list(ticker_list_path)

  while True:
    cur_time = datetime.datetime.now()
    time_string = str(cur_time.year) + ":" + str(cur_time.month) + ":" + str(cur_time.day) + ":" + str(cur_time.minute) + ":" + str(cur_time.second)
    for ticker_name in ticker_list:
      stock_price = float(random.randint(20, 20000)/10)
      stock_record = {"ticker": ticker_name, "price": stock_price, "time": time_string}
#      json.dump(stock_record, output_user_filename)
#      d = yaml.safe_load(line)
#      jd = json.dumps(d)
 
        # send the messages to separate topics
      producer_user.send(sys.argv[1], stock_record) 
    time.sleep(wait_time)     

if __name__ == "__main__":
  main()