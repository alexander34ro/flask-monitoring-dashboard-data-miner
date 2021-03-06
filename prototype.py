from collections import namedtuple
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from typing import List
import math
import psutil

Measurement = namedtuple('Measurement', ['time', 'measurement'])

# DATABASE = 'flask_monitoringdashboard.db'
DATE = '2021-01-08'
SERVER = 'A'
LENGTH = 10
BASE_TRAFFIC_PER_MINUTE = 100
REGRESSION_LEVEL = 2
REGRESSION_MAGNITUDE = 1
WINDOW = 10
WINDOW_SIZE = timedelta(seconds=WINDOW)

DATABASE = 'db_' + DATE + '_' + SERVER + '_length_' + str(LENGTH) + '_traffic_' + str(BASE_TRAFFIC_PER_MINUTE) + '_regression_' + str(REGRESSION_LEVEL) + "_" + str(REGRESSION_MAGNITUDE) + '.db'

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

#####
# CPU data processing
#####

def average_time_from_window(window):
    return window[0][0] + WINDOW_SIZE / 2


def average_cpu_from_window(window):
    return np.average([w[1] for w in window])


def average_for_windows(measurements: List[Measurement]):
    if not measurements: return []

    measurement_delta = measurements[1].time - measurements[0].time
    per_window = int(round(WINDOW_SIZE / measurement_delta))
    count = len(measurements) / per_window
    print(measurement_delta, per_window, count)
    
    windows = np.array_split(measurements, count)
    results = [Measurement(
        time = average_time_from_window(window),
        measurement = average_cpu_from_window(window)
    ) for window in windows]

    return results


#####
# Request data processing
#####

def create_empty_cpu_buckets():
    buckets = {}
    for i in range(101): buckets[i] = []

    return buckets


def time_difference(time1, time2):
    return abs((time1 - time2).total_seconds())


def closest_cpu_measurement(cpu_usage: List[Measurement], time) -> List[Measurement]:
    deltas = [dict(
        time_diff = time_difference(time, c.time),
        cpu_measurement = c.measurement
    ) for c in cpu_usage]
    smallest_delta = sorted(deltas, key=lambda k: k['time_diff'])[0]
    cpu_usage = smallest_delta['cpu_measurement']

    return int(round(cpu_usage))


def populate_buckets(residence_times, buckets, cpu_usage):
    for measurement in residence_times:
        cpu_measurement = closest_cpu_measurement(cpu_usage, measurement.time)
        buckets[cpu_measurement].append(measurement)

    return buckets


def average_latency_for_bucket(buckets, cpu):
    if (len(buckets[cpu]) == 0): return None

    measured_latencies = [m.measurement for m in buckets[cpu]]

    return np.average(measured_latencies)


def requests_per_minute(minute):
    traffic_multiplier = -math.cos(4 * minute / math.pi) + 2

    return traffic_multiplier * BASE_TRAFFIC_PER_MINUTE

#####
# Main
#####

if __name__ == '__main__':
    # Load and process CPU data to get average CPU usage
    cpu_usage_measurements = load_cpu_usage()
    cpu_usage_averages = average_for_windows(cpu_usage_measurements)
    # Load request data
    residence_times = load_residence_times()
    print(len(residence_times))
    # Create CPU usage buckets
    buckets = create_empty_cpu_buckets()
    buckets = populate_buckets(residence_times, buckets, cpu_usage_averages)
    # Average request latency by CPU usage
    latency_per_bucket = [dict(
        latency = average_latency_for_bucket(buckets, cpu),
        cpu_usage = cpu
    ) for cpu in buckets.keys()]
    average_latency = [c for c in sorted(
            latency_per_bucket, key=lambda r: r['cpu_usage']
        ) if c['latency'] and c['latency'] < 1000 and
        c['cpu_usage'] > 5 and c['cpu_usage'] < 95]
    print(average_latency)

    cpu_usages = [r['cpu_usage'] for r in average_latency]
    latencies = [r['latency'] for r in average_latency]

    # Plot request frequency over time
    plt.figure()
    plt.subplot(2, 1, 1)
    times = np.arange(0, LENGTH, 1 / 60)
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
    plt.plot([(measurement.time - start).total_seconds() for measurement in cpu_usage_averages],
             [measurement.measurement for measurement in cpu_usage_averages])

    # Plot CPU usage versus latency
    plt.figure()
    plt.title('CPU usage vs latency')
    plt.plot(cpu_usages, latencies, 'x-')
    x = np.arange(0, 100, 0.01)
    z = np.polyfit(cpu_usages, latencies, 1)
    p = np.poly1d(z)
    plt.plot(x, p(x), 'r--', label='Trend line')
    plt.fill_between(cpu_usages, latencies)
    plt.xlabel('CPU Usage')
    plt.ylabel('Latency')
    plt.legend()
    plt.xlim(0, 100)
    MIN_LATENCY = 0.0
    MAX_LATENCY = 500.0
    plt.ylim(MIN_LATENCY, MAX_LATENCY)
    plt.savefig(SERVER + '_traffic_' + str(BASE_TRAFFIC_PER_MINUTE) + '_window_' + str(WINDOW) + '_regression_' + str(REGRESSION_LEVEL) + '_latency' + '.png')

    # Plot CPU usage vs service time
    plt.figure()
    service_times = [c['latency'] * (1 - c['cpu_usage'] / 100)
                     for c in average_latency]
    cpu_usages = [c['cpu_usage'] for c in average_latency]
    plt.plot(cpu_usages, service_times)
    plt.fill_between(cpu_usages, service_times)
    plt.xlabel('CPU usage')
    plt.ylabel('Service time')
    plt.axhline(y=np.median(service_times), color='r', linestyle='-',
                label='Median service time')
    plt.legend()
    plt.xlim(0, 100)
    MIN_LATENCY = 0.0
    MAX_LATENCY = 350.0
    plt.ylim(MIN_LATENCY, MAX_LATENCY)
    plt.savefig(SERVER + '_traffic_' + str(BASE_TRAFFIC_PER_MINUTE) + '_window_' +
                str(WINDOW) + '_regression_' +
                str(REGRESSION_LEVEL) + '_service' + '.png')
    
    plt.show()
