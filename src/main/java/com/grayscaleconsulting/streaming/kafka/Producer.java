package com.grayscaleconsulting.streaming.kafka;

import com.grayscaleconsulting.streaming.data.metadata.KeyValue;

/**
 * Broadcasts values set to the main datastore.
 *
 * Created by ivaramme on 7/28/15.
 */
public interface Producer {
    void publish(KeyValue value);

    void shutdown();
}
