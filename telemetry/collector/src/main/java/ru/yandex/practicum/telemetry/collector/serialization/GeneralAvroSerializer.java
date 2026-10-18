package ru.yandex.practicum.telemetry.collector.serialization;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class GeneralAvroSerializer implements Serializer<Object> {
    EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            SpecificRecord record = (SpecificRecord) data;
            SpecificDatumWriter<SpecificRecord> writer =
                    new SpecificDatumWriter<>(record.getSchema());
            BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Ошибка сериализации Avro", e);
        }
    }
}