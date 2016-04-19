#!/usr/bin/python

import json
from shapely.geometry import shape, Point
import sys

# load GeoJSON file containing sectors
with open('NTA.geojson', 'r') as f:
    js = json.load(f)

import sys

for line in sys.stdin:
    # Trips data has 14 features,
    # pickup_longitude index 10
    # pickup_latitude index  11
    # dropoff_longitude index 12
    # dropoff_latitude index 13
    # pickup_datetime index 5
    # dropoff_datetime index 6

    line = line.strip()
    line =  line.split(',')

    if line[0] == 'medallion':
        continue

    pickup_yearmonth = line[5][0:10]
    dropoff_yearmonth = line[6][0:10]

    # construct point based on long/lat returned by geocoder
    pick_up_point = Point(float(line[10]), float(line[11]))
    drop_off_point = Point(float(line[12]), float(line[13]))

    # check each polygon to see if it contains the point
    for feature in js['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(pick_up_point):
            pick_up_neighborhood = feature['properties']['ntaname']
        if polygon.contains(drop_off_point):
            drop_off_neighborhood = feature['properties']['ntaname']

    print '%s, %s, %s, %s' % (pickup_yearmonth, pick_up_neighborhood, dropoff_yearmonth, drop_off_neighborhood)
