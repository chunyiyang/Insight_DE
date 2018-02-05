# StockRank

# Description:
To build a stock recommender based on real time price and social media trading information.

# Background:
There is so much information that it's always difficult for people with less financial knowledge to make decision which stock to invest on. 

# Use case:
Not only stock market, but also any situation that needs to do computation, combination on multiple real-time data streamings.

# Challenges:
How to select a suitable database.

# Input:
* Simulated realtime stock price 
* Simulated realtime trading information data stream

# Output:
Dashboard with 3 realtime rank list. 
Table1: Stock rank that sorted by ratio of [current price]/[mean of past one year]
Table2: Stock rank that shows trading popularity.
Table3: Based on table 1 ranking and table 2 ranking.
![alt text]https://github.com/chunyiyang/Insight_DE/blob/master/Images/app_UI.png


# Pipeline:
Kafka, Spark Streaming, Cassandra, Flask
![alt text]https://github.com/chunyiyang/Insight_DE/blob/master/Images/Pipeline.png


