package ru.yandex.practicum.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.hub.*;

import java.util.List;

@Component
public class HubEventMapper {

    public HubEventAvro toAvro(HubEvent event) {
        Object payload = switch (event.getType()) {
            case DEVICE_ADDED -> toDeviceAdded((DeviceAddedEvent) event);
            case DEVICE_REMOVED -> toDeviceRemoved((DeviceRemovedEvent) event);
            case SCENARIO_ADDED -> toScenarioAdded((ScenarioAddedEvent) event);
            case SCENARIO_REMOVED -> toScenarioRemoved((ScenarioRemovedEvent) event);
        };

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private DeviceAddedEventAvro toDeviceAdded(DeviceAddedEvent e) {
        return DeviceAddedEventAvro.newBuilder()
                .setId(e.getId())
                .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                .build();
    }

    private DeviceRemovedEventAvro toDeviceRemoved(DeviceRemovedEvent e) {
        return DeviceRemovedEventAvro.newBuilder()
                .setId(e.getId())
                .build();
    }

    private ScenarioAddedEventAvro toScenarioAdded(ScenarioAddedEvent e) {
        List<ScenarioConditionAvro> conditions = e.getConditions().stream()
                .map(this::toCondition)
                .toList();

        List<DeviceActionAvro> actions = e.getActions().stream()
                .map(this::toAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(e.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioRemovedEventAvro toScenarioRemoved(ScenarioRemovedEvent e) {
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(e.getName())
                .build();
    }

    private ScenarioConditionAvro toCondition(ScenarioCondition c) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(c.getSensorId())
                .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                .setValue(c.getValue())
                .build();
    }

    private DeviceActionAvro toAction(DeviceAction a) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(a.getSensorId())
                .setType(ActionTypeAvro.valueOf(a.getType().name()))
                .setValue(a.getValue())
                .build();
    }
}