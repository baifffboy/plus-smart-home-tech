package ru.yandex.practicum.telemetry.collector.model.hub;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    private String sensorId;

    @NotNull
    private ScenarioConditionType type;

    @NotNull
    private ScenarioConditionOperation operation;

    private Integer value;
}
