package org.zarka.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AvroSerializerDeserializer {
    private static Logger logger = LogManager.getLogger(AvroSerializerDeserializer.class);

    public static byte[] serialize(WeatherData weatherData) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
        DatumWriter<WeatherData> writer = new SpecificDatumWriter<WeatherData>(WeatherData.class);
        try {
            writer.write(weatherData, encoder);
            encoder.flush();
        } catch (IOException e) {
            logger.error("Error serializing record", e);
        }
        return bos.toByteArray();
    }
    
    public static WeatherData deserialize(byte[] bytes) {
        return null;
    }
}
