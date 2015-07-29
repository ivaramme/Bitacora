package com.grayscaleconsulting.streaming.kafka;

import com.grayscaleconsulting.streaming.data.DataManager;

/**
 * Created by ivaramme on 7/28/15.
 */
public interface Consumer {
    void setDataManager(DataManager dataManager);
    void shutdown();
}
