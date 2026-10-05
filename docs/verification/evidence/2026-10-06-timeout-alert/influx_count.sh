#!/bin/bash
# usage: influx_count.sh VEHICLE_ID  -> prints rows(distinct _time) for field speed
V=$1
docker exec -e V="$V" telemetry-influxdb sh -c 'influx query --org "$DOCKER_INFLUXDB_INIT_ORG" --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" --raw "from(bucket: \"$DOCKER_INFLUXDB_INIT_BUCKET\") |> range(start: -30d) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r.vehicle_id == \"$V\" and r._field == \"speed\") |> group() |> count()"' 2>&1 | grep -v '^#' | grep -v '^,result' | grep -v '^$'
