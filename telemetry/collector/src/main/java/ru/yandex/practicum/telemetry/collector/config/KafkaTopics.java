package ru.yandex.practicum.telemetry.collector.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaTopics {
    public static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    public static final String HUBS_TOPIC = "telemetry.hubs.v1";
}
