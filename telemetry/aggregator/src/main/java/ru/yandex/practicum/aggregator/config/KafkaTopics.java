package ru.yandex.practicum.aggregator.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaTopics {
    public static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    public static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";
}