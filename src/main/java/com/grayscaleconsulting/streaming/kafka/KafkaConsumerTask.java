package com.grayscaleconsulting.streaming.kafka;

import com.grayscaleconsulting.streaming.data.metadata.KeyValue;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.io.IOUtils;
import org.mortbay.util.IO;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Created by ivaramme on 6/27/15.
 */
public class KafkaConsumerTask implements Runnable{
    private KafkaStream stream;
    private int threadNumber;
    private ConsumerConnector consumer;
    private Consumer<String> callback;

    public KafkaConsumerTask(ConsumerConnector consumer, KafkaStream stream, int threadNumber, Consumer<String> callback) {
        this.consumer = consumer;
        this.stream = stream;
        this.threadNumber = threadNumber;
        this.callback = callback;
    }
    
    public void run(){
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            try {
                InputStream is = new ByteArrayInputStream(it.next().message());
                callback.accept(IOUtils.toString(is));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.commitOffsets(true);
            }
        }

        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
