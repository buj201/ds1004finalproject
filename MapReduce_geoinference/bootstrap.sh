#!/bin/sh
set -e

mkdir -p /home/hadoop/shapefiles
aws s3 cp s3://buj201-ds1004-sp16/final_project/NTA.geojson /home/hadoop/shapefiles/NTA.geojson
