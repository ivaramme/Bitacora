package com.grayscaleconsulting.bitacora.util;

import com.grayscaleconsulting.bitacora.data.metadata.KeyValue;
import com.grayscaleconsulting.bitacora.rpc.avro.KeyValueRawAvro;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by ivaramme on 8/27/15.
 */
public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    
    public static byte[] convertToAvro(KeyValue keyValue) {
        KeyValueRawAvro rawData = new KeyValueRawAvro();
        rawData.setKey(keyValue.getKey());
        rawData.setSource(keyValue.getSource());
        rawData.setTimestamp(keyValue.getTimestamp());
        rawData.setTtl(keyValue.getTtl());
        rawData.setUuid(keyValue.getUuid());
        rawData.setValue(keyValue.getValue());

        try {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(bao, null);
            new SpecificDatumWriter<>(rawData.getSchema()).write(rawData, encoder);
            encoder.flush();
            
            return bao.toByteArray();
        } catch (IOException e) {
            logger.error("Error converting message to Raw Bytes");
        }
        
        return null;
    }
    
    public static KeyValue convertToKeyValue(byte[] bytes) {
        try {
            KeyValueRawAvro rawValue = new SpecificDatumReader<>(KeyValueRawAvro.class).read(
                    null,
                    DecoderFactory.get().binaryDecoder(bytes, null)
            );
            
            return KeyValue.createNewKeyValue(rawValue.getKey().toString(),
                    rawValue.getValue().toString(), rawValue.getTimestamp(), rawValue.getTtl(), rawValue.getSource());
        } catch (IOException ex) {
            logger.error("Error deserializing message from Kafka: {}", ex);
        }
        
        return null;
    }

}
