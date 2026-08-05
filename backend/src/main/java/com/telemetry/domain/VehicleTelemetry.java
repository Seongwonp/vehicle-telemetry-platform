package com.telemetry.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.util.List;

@Data
public class VehicleTelemetry {

    @JsonProperty("vehicle_id")
    @NotBlank
    @Pattern(regexp = "^[A-Z0-9-]{4,20}$")
    private String vehicleId;

    @NotBlank
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

    @Valid
    private GpsLocation gps;

    @JsonProperty("dtc_codes")
    private List<String> dtcCodes;

    @Data
    public static class GpsLocation {
        @DecimalMin("-90.0")
        @DecimalMax("90.0")
        private double lat;

        @DecimalMin("-180.0")
        @DecimalMax("180.0")
        private double lng;
    }
}
