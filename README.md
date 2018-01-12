# Insight_DE

# Description:
To build a realtime data pipeline for huge voice file.

# Dataset:
* log file that includes voice file id, call number, call start time, call end time, business category, location
* voice file
Total size: several hundreds GB

# Current Situation:
Voice recognition algorithm can only be run on a very small amount of data. Need to manually update the model and no realtime response.

# Target:
Run the ML algorithm on a much larger dataset and integrate realtime dataprocessing.

# Challenge:
In some country, different areas have different accent. Need to categorize all the calls into different groups due to the area code.

# Usecase:
To improve the call center AI performance and to automatically update ML model.

# Architecture:
SPARK , SPARK STREAMING, PostgreSQL

