package ru.yandex.practicum.aggregator.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.config.KafkaTopics;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AggregationStarter {

    KafkaConsumer<String, SensorEventAvro> consumer;
    KafkaProducer<String, SpecificRecordBase> producer;
    SnapshotAggregator aggregator;

    public void start() {
        try {
            consumer.subscribe(List.of(KafkaTopics.SENSORS_TOPIC));
            log.info("Aggregator подписан на топик {}", KafkaTopics.SENSORS_TOPIC);

            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    if (event == null) {
                        continue;
                    }

                    Optional<SensorsSnapshotAvro> updated = aggregator.updateState(event);

                    if (updated.isPresent()) {
                        SensorsSnapshotAvro snapshot = updated.get();
                        ProducerRecord<String, SpecificRecordBase> outRecord =
                                new ProducerRecord<>(
                                        KafkaTopics.SNAPSHOTS_TOPIC,
                                        snapshot.getHubId(),
                                        snapshot
                                );
                        producer.send(outRecord, (metadata, ex) -> {
                            if (ex != null) {
                                log.error("Ошибка отправки снапшота в Kafka", ex);
                            } else {
                                log.info("Снапшот отправлен: topic={}, partition={}, offset={}",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                    }

                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                    currentOffsets.clear();
                }
            }

        } catch (WakeupException ignored) {
            log.info("Получен WakeupException — завершаем работу Aggregator");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                log.info("Сбрасываем буфер продюсера...");
                producer.flush();

                log.info("Фиксируем смещения консьюмера...");
                consumer.commitSync();

            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}
