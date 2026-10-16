package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;


@Service
@Slf4j
@RequiredArgsConstructor
public class CollectorServiceImpl implements CollectorService {

    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final SensorEventMapper sensorEventMapper;
    private final HubEventMapper hubEventMapper;

    @Override
    public void collectSensorEvent(SensorEvent event) {
        SensorEventAvro avro = sensorEventMapper.toAvro(event);

        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(KafkaTopics.SENSORS_TOPIC, avro.getId().toString(), avro);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки sensor-события в Kafka: {}", exception.getMessage(), exception);
            } else {
                log.info("Sensor-событие отправлено: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void collectHubEvent(HubEvent event) {
        HubEventAvro avro = hubEventMapper.toAvro(event);

        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(KafkaTopics.HUBS_TOPIC, avro.getHubId().toString(), avro);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки hub-события в Kafka: {}", exception.getMessage(), exception);
            } else {
                log.info("Hub-событие отправлено: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
}