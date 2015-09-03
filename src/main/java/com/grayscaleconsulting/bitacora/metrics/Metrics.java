package com.grayscaleconsulting.bitacora.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

/**
 * Created by ivaramme on 8/9/15.
 */
public class Metrics {
    public static MetricsRegistry registry;

    public static MetricsRegistry getDefault() {
        if(null == registry)
            initJmxReporter();
        
        return registry;
    }
    
    public static void initJmxReporter() {
        if(null == registry) {
            Metrics.registry = new MetricsRegistry();
            JmxReporter.startDefault(registry);
        }
    }
}
