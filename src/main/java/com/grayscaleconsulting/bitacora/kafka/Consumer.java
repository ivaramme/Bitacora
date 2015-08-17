package com.grayscaleconsulting.bitacora.kafka;

import com.grayscaleconsulting.bitacora.data.DataManager;

/**
 * Created by ivaramme on 7/28/15.
 */
public interface Consumer {
    void setDataManager(DataManager dataManager);
    void shutdown();
}
