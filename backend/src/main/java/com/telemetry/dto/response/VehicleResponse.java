package com.telemetry.dto.response;

import com.telemetry.entity.Vehicle;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
@Schema(description = "차량 정보 응답")
public class VehicleResponse {

    @Schema(description = "DB PK")
    private final Long id;

    @Schema(description = "차량 ID", example = "KR-GA-1234")
    private final String vehicleId;

    @Schema(description = "차량 이름", example = "현대 아반떼")
    private final String name;

    @Schema(description = "소유자", example = "홍길동")
    private final String owner;

    @Schema(description = "활성 여부")
    private final boolean active;

    @Schema(description = "등록 일시")
    private final LocalDateTime registeredAt;

    public VehicleResponse(Vehicle vehicle) {
        this.id = vehicle.getId();
        this.vehicleId = vehicle.getVehicleId();
        this.name = vehicle.getName();
        this.owner = vehicle.getOwner();
        this.active = vehicle.isActive();
        this.registeredAt = vehicle.getRegisteredAt();
    }
}
