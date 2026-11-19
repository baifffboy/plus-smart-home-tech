package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class SnapshotAggregator {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();

        // 1. Получаем или создаём снапшот
        SensorsSnapshotAvro snapshot = snapshots.get(hubId);
        if (snapshot == null) {
            snapshot = SensorsSnapshotAvro.newBuilder()
                    .setHubId(hubId)
                    .setTimestamp(event.getTimestamp())
                    .setSensorsState(new HashMap<>())
                    .build();
            snapshots.put(hubId, snapshot);
        }

        // 2. Проверяем, есть ли данные для этого датчика
        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());

        if (oldState != null) {
            // 2a. Событие старее или равно — игнорируем
            if (!oldState.getTimestamp().isBefore(event.getTimestamp())) {
                log.debug("Пропускаем устаревшее событие: sensorId={}, eventTs={}, oldTs={}",
                        event.getId(), event.getTimestamp(), oldState.getTimestamp());
                return Optional.empty();
            }

            // 2b. Данные не изменились — дедупликация
            if (oldState.getData().equals(event.getPayload())) {
                log.debug("Данные не изменились для sensorId={}, пропускаем", event.getId());
                return Optional.empty();
            }
        }

        // 3. Обновляем снапшот
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());

        log.info("Снапшот обновлён: hubId={}, sensorId={}", hubId, event.getId());
        return Optional.of(snapshot);
    }
}
