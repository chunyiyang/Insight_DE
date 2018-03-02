from flask import Flask, render_template, request
from cassandra.cluster import Cluster
import pandas as pd

app = Flask(__name__)
app.debug=True
@app.route('/')

default_weight = 5

def get_window_stock(session):
    """ convert the data from table window_stock into pandas dataframe and sort by column ticker
        Args:   <session: cassandra.cluster.Session>
        Return: table_window_stock: pandas dataframe
    """         
    table_window_stock = session.execute('SELECT ticker, low_price FROM window_stock;')
    table_window_stock = table_window_stock._current_rows
    table_window_stock = table_window_stock.sort_values('ticker') 
    return table_window_stock   

def get_window_user(session):
    """ convert the data from table window_user into pandas dataframe and sort by column sum_qty
        Args:   <session: cassandra.cluster.Session>
        Return: table_window_user: pandas dataframe
    """         
    table_window_user = session.execute('SELECT ticker, sum_qty FROM window_user;')
    table_window_user = table_window_user._current_rows
    table_window_user = table_window_user.sort_values('sum_qty',ascending=False)
    table_window_user = table_window_user.reset_index(drop=True)
    return table_window_user

def get_stock_avg(session):
    """ convert the data from table stock_avg_price into pandas dataframe and sort by column ticker
        Args:   <session: cassandra.cluster.Session>
        Return: table_stock_avg_price: pandas dataframe
    """         
    table_stock_avg_price = session.execute('SELECT ticker, price FROM stock_avg_price')          
    table_stock_avg_price = table_stock_avg_price._current_rows
    table_stock_avg_price = table_stock_avg_price.sort_values('ticker')
    return table_stock_avg_price

def get_table_low_avg(table_window_stock, table_stock_avg_price):
    """ combine dataframe table_window_stock and table_stock_avg_price
        and calcuate the value of low_price/ price
        Args:   <table_window_stock: pandas dataframe>
                <table_stock_avg_price: pandas dataframe>
        Return: table_low_avg: pandas dataframe
    """      
    table_low_avg = pd.concat((table_window_stock,table_stock_avg_price['price']),axis=1)
    table_low_avg['ratio'] = table_low_avg['low_price']/table_low_avg['price']
    table_low_avg = table_low_avg.round({'ratio': 3, 'price': 3})
    table_low_avg = table_low_avg.sort_values('ratio')
    table_low_avg = table_low_avg.reset_index(drop=True)
    return table_low_avg

def get_new_price_rank(table_low_avg):
    """
        Args:   <table_low_avg: pandas dataframe>
        Return: new_price_rank: pandas dataframe
    """    
    new_price_rank= {'ticker': table_low_avg['ticker'], 'ratio_rank': \
    table_low_avg['ratio_rank']}
    new_price_rank = pd.DataFrame(data=new_price_rank)
    new_price_rank = new_price_rank.sort_values('ticker')
    new_price_rank = new_price_rank.reset_index(drop=True)
    return new_price_rank

def get_new_qty_rank(table_window_user):
    """
        Args:   <table_window_user: pandas dataframe>
        Return: new_qty_rank: pandas dataframe
    """    
    new_qty_rank = {'ticker': table_window_user['ticker'], 'qty_rank': \
    table_window_user['qty_rank']}
    new_qty_rank = pd.DataFrame(data=new_qty_rank)
    new_qty_rank = new_qty_rank.sort_values('ticker')
    new_qty_rank = new_qty_rank.reset_index(drop=True)
    return new_qty_rank

def get_combined_rank(new_price_rank, new_qty_rank):
    """ 
        Args:   <new_price_rank: pandas dataframe>
                <new_qty_rank: pandas dataframe>
        Return: combined_rank: pandas dataframe
    """      
   
    combined_rank = pd.concat((new_price_rank,new_qty_rank['qty_rank']),axis=1)
    combined_rank['para_rank'] = (combined_rank['ratio_rank']*(10-weight) \
        + combined_rank['qty_rank']*(weight))/10
    combined_rank = combined_rank.sort_values('para_rank')
    combined_rank = combined_rank.reset_index(drop=True) 
    return combined_rank

def homepage():
    weight = default_weight
    def _pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = _pandas_factory    
    session.default_fetch_size = None

    table_window_stock = get_window_stock(session)

    table_window_user = get_window_user(session)

    table_stock_avg_price = get_stock_avg(session)

    table_low_avg = get_table_low_avg(table_window_stock, table_stock_avg_price)

    table_low_avg['ratio_rank'] = table_low_avg.index
    table_window_user['qty_rank'] = table_window_user.index
    
    new_price_rank = get_new_price_rank(table_low_avg)

    new_qty_rank = get_new_qty_rank(table_window_user)

    combined_rank = get_combined_rank(new_price_rank, new_qty_rank)

    return render_template("example.html", **locals())
             
if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('mykeyspace')
    app.run(host='0.0.0.0',debug=True, port=5000)  


