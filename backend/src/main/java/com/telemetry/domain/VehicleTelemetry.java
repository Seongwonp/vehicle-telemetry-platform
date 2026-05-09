package com.telemetry.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class VehicleTelemetry {

    @JsonProperty("vehicle_id")
    private String vehicleId;

    private String timestamp;

    private double speed;

    private int rpm;

    @JsonProperty("engine_temp")
    private double engineTemp;

    @JsonProperty("throttle_position")
    private double throttlePosition;

    @JsonProperty("fuel_level")
    private double fuelLevel;

    @JsonProperty("battery_voltage")
    private double batteryVoltage;

    private GpsLocation gps;

    @JsonProperty("dtc_codes")
    private List<String> dtcCodes;

    @Data
    public static class GpsLocation {
        private double lat;
        private double lng;
    }
}
