from collections import namedtuple
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from typing import List
import math
import psutil

Measurement = namedtuple('Measurement', ['time', 'measurement'])
MeasurementWindow = namedtuple('MeasurementWindow', ['time', 'measurements'])

DATABASE = 'db_length_10_traffic_20_regression_2.db'
WINDOW_SIZE = timedelta(seconds=20)
BASE_TRAFFIC_PER_MINUTE = 20

#####
# DB Management
#####


def load_db_cursor():
    return sqlite3.connect(DATABASE).cursor()


def load_measurements_from_db(query):
    cursor = load_db_cursor()
    cursor.execute(query)
    return cursor.fetchall()


def format_date(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')


def load_cpu_usage() -> List[Measurement]:
    resultset = load_measurements_from_db(
        'SELECT * FROM CustomGraphData ORDER BY time ASC'
    )

    return [Measurement(
        time=format_date(d[2]),
        measurement=d[3]
    ) for d in resultset]


def load_residence_times() -> List[Measurement]:
    resultset = load_measurements_from_db(
        'SELECT duration, time_requested FROM Request ORDER BY time_requested ASC'
    )

    return [Measurement(
        time=format_date(d[1]),
        measurement=d[0]
    ) for d in resultset]


def segment_into_windows(measurements: List[Measurement]):
    if not measurements: return []

    measurement_delta = measurements[1].time - measurements[0].time
    per_window = int(round(WINDOW_SIZE / measurement_delta))
    count = len(measurements) / per_window
    
    windows = np.array_split(np.array(measurements), count)
    results = [MeasurementWindow(
        time=window[0][0] + WINDOW_SIZE / 2,
        measurements = [Measurement(
            time = w[0],
            measurement = w[1]
        ) for w in window]
    ) for window in windows]

    return results


def requests_per_minute(minute):
    traffic_multiplier = -math.cos(4 * minute / math.pi) + 2

    return traffic_multiplier * BASE_TRAFFIC_PER_MINUTE


def create_empty_cpu_bucket():
    bucket = {}

    for i in range(100):
        bucket[i + 1] = []

    return bucket


def find_nearby_cpu_measurement(cpu_usage_averages: List[Measurement], time) \
        -> List[Measurement]:
    diffs = [
        dict(seconds_diff=abs((c.time - time).total_seconds()),
             cpu_measurement=c.measurement)
        for c in cpu_usage_averages]

    return sorted(diffs, key=lambda k: k['seconds_diff'])[0]['cpu_measurement']


if __name__ == '__main__':
    # Load and process CPU data
    cpu_usage_measurements = load_cpu_usage()
    cpu_usage_segments = segment_into_windows(cpu_usage_measurements)

    cpu_usage_averages = [Measurement(time=segment.time, measurement=np.average(
        [m.measurement for m in segment.measurements])) for segment in cpu_usage_segments]

    residence_times = load_residence_times()

    bucket = create_empty_cpu_bucket()

    for measurement in residence_times:
        nearby_cpu_usage = find_nearby_cpu_measurement(
            cpu_usage_averages,
            measurement.time
        )

        if math.isnan(nearby_cpu_usage): continue
        
        floored_cpu_usage = math.floor(nearby_cpu_usage)
        bucket[floored_cpu_usage].append(measurement)

    results = [dict(latency=np.average([m.measurement for m in bucket[k]]) if len(
        bucket[k]) > 0 else None, cpu_usage=k) for k in
               bucket.keys()]

    results = [c for c in sorted(results, key=lambda r: r['cpu_usage']) if c['latency']]
    cpu_usages = [r['cpu_usage'] for r in results]
    latencies = [r['latency'] for r in results]

    plt.figure()
    plt.subplot(2, 1, 1)
    times = np.arange(0, 10, 1 / 60)
    rm = [requests_per_minute(t) for t in times]
    plt.plot([t*60 for t in times], rm)
    plt.axhline(y=min(rm), linestyle='--', label='Minimum RPM')
    print(max(rm))
    plt.axhline(y=max(rm), linestyle='--', label='Maximum RPM')
    plt.title('RPM over time')
    plt.xlabel('Time in seconds since start simulation')
    plt.ylabel('RPM')
    plt.legend()

    # Plot CPU usage over time
    plt.subplot(2, 1, 2)
    plt.xticks(rotation=45)
    plt.title('CPU usage over time')
    plt.xlabel('Time in seconds since start simulation')
    start = cpu_usage_averages[0].time
    plt.plot([(measurement.time - start).total_seconds() for measurement in
              cpu_usage_averages],
             [measurement.measurement for measurement in cpu_usage_averages])

    # Plot CPU usage versus latency
    plt.figure()
    plt.title('CPU usage vs latency')
    plt.plot(cpu_usages, latencies, 'x-')
    z = np.polyfit(cpu_usages, latencies, 1)
    p = np.poly1d(z)
    plt.plot(cpu_usages, p(cpu_usages), 'r--', label='Trend line')
    plt.xlabel('CPU Usage')
    plt.ylabel('Latency')
    plt.legend()

    # Plot CPU usage vs service time
    plt.figure()
    service_times = [c['latency'] * (1 - c['cpu_usage'] / 100) for c in results]
    plt.plot([c['cpu_usage'] for c in results], service_times)
    plt.xlabel('CPU usage')
    plt.ylabel('Service time')
    plt.axhline(y=np.median(service_times), color='r', linestyle='-',
                label='Median service time')
    plt.legend()
    # plt.x

    plt.show()
