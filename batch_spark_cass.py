from cassandra.cluster import Cluster
import datetime
import timeit

if __name__ == '__main__':
	cluster = Cluster(control_connection_timeout=20.0)
	session = cluster.connect('mykeyspace')

#	Calculate daily average and update the year_stock_price table

	rows = session.execute('SELECT ticker, avg(price) AS price FROM day_stock_price GROUP BY ticker;',timeout=None)


	cur_time = datetime.datetime.now()
	time_string = str(cur_time.year) + ":" + str(cur_time.month).zfill(2) + ":" + str(cur_time.day).zfill(2)

	for row in rows:
		ticker_name = row.ticker
		ticker_price = row.price 
		session.execute("INSERT INTO year_stock_price (ticker, time, price) VALUES ('{}', '{}',{});"\
			.format(ticker_name, time_string, ticker_price))	

#	From stock_avg_price table calcalate the price average value"""
	session.execute('CREATE TABLE IF NOT EXISTS stock_avg_price(ticker text, price int, PRIMARY KEY (ticker) );')
	rows = session.execute('SELECT ticker, avg(price) AS price FROM year_stock_price GROUP BY ticker;')
	for row in rows:
		ticker_name = row.ticker
		ticker_price = row.price
		session.execute("INSERT INTO stock_avg_price (ticker, price) VALUES ('{}', {});".format(ticker_name, ticker_price))