from flask import Flask, render_template, request
from cassandra.cluster import Cluster
import pandas as pd
import timeit

app = Flask(__name__)
app.debug=True
@app.route('/')


def homepage():
    start = timeit.default_timer()
    weight = 5
    def _pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    cluster = Cluster()
    session = cluster.connect('mykeyspace')
    session.row_factory = _pandas_factory
    session.default_fetch_size = None
    table_window_stock = session.execute('SELECT ticker, low_price FROM window_stock;')
    table_window_stock = table_window_stock._current_rows
    table_window_stock = table_window_stock.sort_values('ticker')

    table_window_user = session.execute('SELECT ticker, sum_qty FROM window_user;')
    table_window_user = table_window_user._current_rows
    table_window_user = table_window_user.sort_values('sum_qty',ascending=False)
    table_window_user = table_window_user.reset_index(drop=True)


    table_stock_avg_price = session.execute('SELECT ticker, price FROM stock_avg_price')          
    table_stock_avg_price = table_stock_avg_price._current_rows
    table_stock_avg_price = table_stock_avg_price.sort_values('ticker')

    table_low_avg = pd.concat((table_window_stock,table_stock_avg_price['price']),axis=1)
    table_low_avg['ratio'] = table_low_avg['low_price']/table_low_avg['price']
    table_low_avg = table_low_avg.round({'ratio': 3, 'price': 3})
    table_low_avg = table_low_avg.sort_values('ratio')
    table_low_avg = table_low_avg.reset_index(drop=True)

    table_low_avg['ratio_rank'] = table_low_avg.index
    table_window_user['qty_rank'] = table_window_user.index
    
    new_price_rank= {'ticker': table_low_avg['ticker'], 'ratio_rank': table_low_avg['ratio_rank']}
    new_price_rank = pd.DataFrame(data=new_price_rank)
    new_price_rank = new_price_rank.sort_values('ticker')
    new_price_rank = new_price_rank.reset_index(drop=True)

    new_qty_rank = {'ticker': table_window_user['ticker'], 'qty_rank': table_window_user['qty_rank']}
    new_qty_rank = pd.DataFrame(data=new_qty_rank)
    new_qty_rank = new_qty_rank.sort_values('ticker')
    new_qty_rank = new_qty_rank.reset_index(drop=True)

    combined_rank = pd.concat((new_price_rank,new_qty_rank['qty_rank']),axis=1)
    combined_rank['para_rank'] = (combined_rank['ratio_rank']*(10-weight) + combined_rank['qty_rank']*(weight))/10
    combined_rank = combined_rank.sort_values('para_rank')
    combined_rank = combined_rank.reset_index(drop=True)
    

    return render_template("example.html", **locals())
             
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, port=5000)  


