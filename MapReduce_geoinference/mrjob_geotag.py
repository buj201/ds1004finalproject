from mrjob.job import MRJob
import json
from shapely.geometry import shape, Point


# load GeoJSON file containing sectors
with open('NTA.geojson', 'r') as f:
    js = json.load(f)

class MRGeotag(MRJob):

    def mapper(self, _, line):

        line =  line.split(',')

        if line[0] != 'medallion':

            dropoff_yearmonth = line[6][0:8]
            pickup_yearmonth = line[5][0:8]

            # construct point based on long/lat returned by geocoder
            pick_up_point = Point(float(line[10]), float(line[11]))
            drop_off_point = Point(float(line[12]), float(line[13]))

            drop_off_neighborhood = None
            pick_up_neighborhood = None

            # check each polygon to see if it contains the point
            for feature in js['features']:
                polygon = shape(feature['geometry'])
                if polygon.contains(drop_off_point):
                    drop_off_neighborhood = feature['properties']['ntaname']
                if polygon.contains(pick_up_point):
                    pick_up_neighborhood = feature['properties']['ntaname']

            pick_up_key = ('pickup', pick_up_neighborhood, pickup_yearmonth)
            drop_off_key = ('dropoff', drop_off_neighborhood, dropoff_yearmonth)

            yield pick_up_key, 1
            yield drop_off_key, 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRGeotag.run()
