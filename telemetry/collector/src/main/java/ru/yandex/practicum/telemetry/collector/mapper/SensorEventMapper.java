package ru.yandex.practicum.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.model.sensor.*;

@Component
public class SensorEventMapper {

    public SensorEventAvro toAvro(SensorEvent event) {
        Object payload = switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT -> toClimateAvro((ClimateSensorEvent) event);
            case LIGHT_SENSOR_EVENT -> toLightAvro((LightSensorEvent) event);
            case MOTION_SENSOR_EVENT -> toMotionAvro((MotionSensorEvent) event);
            case SWITCH_SENSOR_EVENT -> toSwitchAvro((SwitchSensorEvent) event);
            case TEMPERATURE_SENSOR_EVENT -> toTemperatureAvro((TemperatureSensorEvent) event);
        };

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private ClimateSensorAvro toClimateAvro(ClimateSensorEvent e) {
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(e.getTemperatureC())
                .setHumidity(e.getHumidity())
                .setCo2Level(e.getCo2Level())
                .build();
    }

    private LightSensorAvro toLightAvro(LightSensorEvent e) {
        return LightSensorAvro.newBuilder()
                .setLinkQuality(e.getLinkQuality())
                .setLuminosity(e.getLuminosity())
                .build();
    }

    private MotionSensorAvro toMotionAvro(MotionSensorEvent e) {
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(e.getLinkQuality())
                .setMotion(e.isMotion())
                .setVoltage(e.getVoltage())
                .build();
    }

    private SwitchSensorAvro toSwitchAvro(SwitchSensorEvent e) {
        return SwitchSensorAvro.newBuilder()
                .setState(e.getState())
                .build();
    }

    private TemperatureSensorAvro toTemperatureAvro(TemperatureSensorEvent e) {
        return TemperatureSensorAvro.newBuilder()
                .setId(e.getId())
                .setHubId(e.getHubId())
                .setTimestamp(e.getTimestamp())
                .setTemperatureC(e.getTemperatureC())
                .setTemperatureF(e.getTemperatureF())
                .build();
    }
}
