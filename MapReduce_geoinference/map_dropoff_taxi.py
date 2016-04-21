#!/usr/bin/python

import json
from shapely.geometry import shape, Point
import sys

# load GeoJSON file containing sectors
with open('/home/hadoop/shapefiles/NTA.geojson', 'r') as f:
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

    dropoff_yearmonth = line[6][0:10]

    # construct point based on long/lat returned by geocoder
    drop_off_point = Point(float(line[12]), float(line[13]))

    # check each polygon to see if it contains the point
    for feature in js['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(drop_off_point):
            drop_off_neighborhood = feature['properties']['ntaname']

    print '%s,%s\t1' % (drop_off_neighborhood, dropoff_yearmonth)
