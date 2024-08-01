import pandas as pd

def process_data(file_path):
    df = pd.read_csv(file_path)
    df.dropna(inplace=True)
    df['trip_duration'] = (pd.to_datetime(df['dropoff_time']) - pd.to_datetime(df['pickup_time'])).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    daily_aggregates = df.groupby(df['pickup_time'].dt.date).agg({
        'trip_duration': 'count',
        'fare_amount': 'mean'
    }).rename(columns={'trip_duration': 'total_trips', 'fare_amount': 'average_fare'})
    return daily_aggregates

processed_data = process_data('nyc_taxi_data_2019.csv')
print(processed_data.head())
