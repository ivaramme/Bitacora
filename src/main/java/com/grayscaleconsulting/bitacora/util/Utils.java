package com.grayscaleconsulting.bitacora.util;

import com.grayscaleconsulting.bitacora.model.KeyValue;
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
import java.util.Properties;

/**
 * Created by ivaramme on 8/27/15.
 */
public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    
    public static final String BASE_NODE_NAME = "/bitacora";
    public static final String AVAILABLE_SERVERS = BASE_NODE_NAME+"/servers";
    public static final String SOCKET_AVAILABLE_SERVERS = BASE_NODE_NAME+"/sockets";
    public static final String SOCKET_CONNECTION_COUNT = Utils.SOCKET_AVAILABLE_SERVERS + "/connections";
    
    public static Properties mainProperties;

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

    public static Properties loadProperties() {
        if(null != mainProperties) {
            return mainProperties;
        }
        
        Properties prop = new Properties();
        try {
            prop.load(Utils.class.getClassLoader().getResourceAsStream("bitacora.properties"));
            logger.info("Bitacora properties file loaded successfully.");
        }
        catch (IOException ex) {
            logger.error("Unable to load properties file: {}", ex);
        }
        return prop;
    }


}
