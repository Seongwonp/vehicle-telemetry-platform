package com.telemetry.controller;

import com.telemetry.service.AnomalyService;
import com.telemetry.service.TelemetryQueryService;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class QueryLimitValidationTest {

    @Test
    void telemetryLimitMustBeBetweenOneAndOneHundred() throws Exception {
        TelemetryController controller = new TelemetryController(mock(TelemetryQueryService.class));
        Method method = TelemetryController.class.getMethod("getRecent", String.class, int.class);
        var executableValidator = Validation.buildDefaultValidatorFactory()
            .getValidator().forExecutables();

        assertThat(executableValidator.validateParameters(
            controller, method, new Object[]{"KR-GA-1234", 0})).isNotEmpty();
        assertThat(executableValidator.validateParameters(
            controller, method, new Object[]{"KR-GA-1234", 101})).isNotEmpty();
        assertThat(executableValidator.validateParameters(
            controller, method, new Object[]{"KR-GA-1234", 20})).isEmpty();
    }

    @Test
    void anomalyLimitMustBeBetweenOneAndOneHundred() throws Exception {
        AnomalyController controller = new AnomalyController(mock(AnomalyService.class));
        Method method = AnomalyController.class.getMethod("getAnomalies", String.class, int.class);
        var executableValidator = Validation.buildDefaultValidatorFactory()
            .getValidator().forExecutables();

        assertThat(executableValidator.validateParameters(
            controller, method, new Object[]{"KR-GA-1234", -1})).isNotEmpty();
        assertThat(executableValidator.validateParameters(
            controller, method, new Object[]{"KR-GA-1234", 20})).isEmpty();
    }
}
