package ru.yandex.practicum.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEventProto event) {
        Object payload = switch (event.getPayloadCase()) {
            case DEVICE_ADDED -> toDeviceAdded(event.getDeviceAdded());
            case DEVICE_REMOVED -> toDeviceRemoved(event.getDeviceRemoved());
            case SCENARIO_ADDED -> toScenarioAdded(event.getScenarioAdded());
            case SCENARIO_REMOVED -> toScenarioRemoved(event.getScenarioRemoved());
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException(
                    "Не задан payload для hub-события: " + event.getHubId());
        };

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(toInstant(event.getTimestamp()))
                .setPayload(payload)
                .build();
    }

    private Instant toInstant(com.google.protobuf.Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private DeviceAddedEventAvro toDeviceAdded(DeviceAddedEventProto e) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(e.getId())
                .setType(DeviceTypeAvro.valueOf(e.getType().name()))
                .build();
    }

    private DeviceRemovedEventAvro toDeviceRemoved(DeviceRemovedEventProto e) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(e.getId())
                .build();
    }

    private ScenarioAddedEventAvro toScenarioAdded(ScenarioAddedEventProto e) {
        List<ScenarioConditionAvro> conditions = e.getConditionList().stream()
                .map(this::toCondition)
                .toList();

        List<DeviceActionAvro> actions = e.getActionList().stream()
                .map(this::toAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(e.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioRemovedEventAvro toScenarioRemoved(ScenarioRemovedEventProto e) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(e.getName())
                .build();
    }

    private ScenarioConditionAvro toCondition(ScenarioConditionProto c) {
        Object value = switch (c.getValueCase()) {
            case BOOL_VALUE -> c.getBoolValue();
            case INT_VALUE -> c.getIntValue();
            case VALUE_NOT_SET -> null;
        };

        return ScenarioConditionAvro.newBuilder()
                .setSensorId(c.getSensorId())
                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro toAction(DeviceActionProto a) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(a.getSensorId())
                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                .setValue(a.hasValue() ? a.getValue() : null)
                .build();
    }
}