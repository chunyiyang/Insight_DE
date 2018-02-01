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
total_user_size = 10000
max_buysell_qty = 10000
ticker_size = 3279

def create_stock_list(filename):
    """ Open a csv file that lists up all the stock ticker symbols
    and converts to a list """
    
    myfile = open(filename, "r")
    ticker_list = []
    
    for line in myfile:
        tickers = line.strip().split(',')
        ticker_list.append(tickers[0])
        
    myfile.close()    
    return ticker_list

def simulate_stock_ticker():
    """ To generate a ticker that has buy/sell stock action """
    ticker_id = random.randint(0,ticker_size)
    return ticker_id

def simulate_action_user_list(total_user_size, action_user_size):
    """ To generate a user_id list that has buy/sell stock action """
    action_user_list = random.sample(range(total_user_size), action_user_size)
    return action_user_list

def simulate_buysell_action():
    """ To generate a action of buy or sell stock action.
    0 for buy, 1 for sell"""
    action = random.randint(0,1)
    return action

def main():
  """Reads input file and sends the lines as json to Kafka
     with optional delay between messages"""

  if len(sys.argv) < 2:
    print "Usage: ./kafka_producer_user.py user-topic"
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

  # Read the file over and over and send the messages line by line
  
  #while forever:
  # Open file and send the messages line by line
  while True:
    action_user_size = random.randint(int(total_user_size / 1000),int(total_user_size / 100))
    action_user_list = simulate_action_user_list(total_user_size, action_user_size)
    ticker_id = simulate_stock_ticker()
    cur_time = datetime.datetime.now()
    time_string = str(cur_time.year) + ":" + str(cur_time.month) + ":" + str(cur_time.day) + ":" + str(cur_time.minute) + ":" + str(cur_time.second)
    
    for user_id in action_user_list:
        ticker_name = ticker_list[ticker_id]
        action = simulate_buysell_action()
        stk_qty = random.randint(1, max_buysell_qty)
#       record = str(user_id) + "\t" + str(action) + "\t" + ticker_name + "\t" + str(stk_qty)  + "\n"
#        user_record = {"user_action": {"uid": user_id, "action": action, "ticker": ticker_name, "qty":stk_qty, "time":time_string}}        
        user_record = {"uid": user_id, "action": action, "ticker": ticker_name, "qty":stk_qty, "time":time_string}
 #       json.dump(user_record, output_user_filename)
 #       d = yaml.safe_load(line)
 #       jd = json.dumps(d)

        # send the messages to separate topics
        producer_user.send(sys.argv[1], user_record) 
    
    for ticker_name in ticker_list:
        user_record = {"uid": action_user_list[0], "action": 0, "ticker": ticker_name, "qty":0, "time":time_string}
        producer_user.send(sys.argv[1], user_record) 
#
    time.sleep(wait_time)     

if __name__ == "__main__":
  main()