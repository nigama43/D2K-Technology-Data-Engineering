import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Create a database connection
engine = create_engine('postgresql://username:password@localhost:5432/nyc_taxi')

# Peak Hours for Taxi Usage
query_peak_hours = """
SELECT 
    EXTRACT(HOUR FROM pickup_datetime) AS hour_of_day,
    COUNT(*) AS trip_count
FROM 
    trips
GROUP BY 
    EXTRACT(HOUR FROM pickup_datetime)
ORDER BY 
    trip_count DESC;
"""
df_peak_hours = pd.read_sql(query_peak_hours, engine)

# Effect of Passenger Count on Trip Fare
query_passenger_fare = """
SELECT 
    passenger_count,
    AVG(total_amount) AS avg_fare
FROM 
    trips
GROUP BY 
    passenger_count
ORDER BY 
    passenger_count;
"""
df_passenger_fare = pd.read_sql(query_passenger_fare, engine)

# Trends in Usage Over the Year
query_trends = """
SELECT 
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    COUNT(*) AS trip_count
FROM 
    trips
GROUP BY 
    EXTRACT(YEAR FROM pickup_datetime), 
    EXTRACT(MONTH FROM pickup_datetime)
ORDER BY 
    year, month;
"""
df_trends = pd.read_sql(query_trends, engine)
df_trends['date'] = pd.to_datetime(df_trends[['year', 'month']].assign(day=1))

# Plot Peak Hours for Taxi Usage
df_peak_hours.plot(kind='bar', x='hour_of_day', y='trip_count', legend=False)
plt.xlabel('Hour of Day')
plt.ylabel('Number of Trips')
plt.title('Peak Hours for Taxi Usage')
plt.show()

# Plot Effect of Passenger Count on Trip Fare
df_passenger_fare.plot(kind='bar', x='passenger_count', y='avg_fare', legend=False)
plt.xlabel('Passenger Count')
plt.ylabel('Average Fare ($)')
plt.title('Effect of Passenger Count on Trip Fare')
plt.show()

# Plot Trends in Usage Over the Year
df_trends.plot(kind='line', x='date', y='trip_count', legend=False)
plt.xlabel('Date')
plt.ylabel('Number of Trips')
plt.title('Trends in Taxi Usage Over the Year')
plt.show()


























