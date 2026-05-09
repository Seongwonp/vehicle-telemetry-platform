package com.telemetry.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

@Data
@Schema(description = "차량 등록 요청")
public class VehicleRegisterRequest {

    @NotBlank(message = "차량 ID는 필수입니다")
    @Pattern(regexp = "^[A-Z0-9-]{4,20}$", message = "차량 ID는 대문자/숫자/하이픈 4~20자")
    @Schema(description = "차량 고유 ID", example = "KR-GA-1234")
    private String vehicleId;

    @NotBlank(message = "차량 이름은 필수입니다")
    @Schema(description = "차량 이름", example = "현대 아반떼")
    private String name;

    @NotBlank(message = "소유자는 필수입니다")
    @Schema(description = "소유자 이름", example = "홍길동")
    private String owner;
}
