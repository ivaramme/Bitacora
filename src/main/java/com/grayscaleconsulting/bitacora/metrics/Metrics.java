package com.grayscaleconsulting.bitacora.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

/**
 * Created by ivaramme on 8/9/15.
 */
public class Metrics {
    public static final MetricsRegistry metrics = new MetricsRegistry();
    private static JmxReporter reporter;

    public static MetricsRegistry getDefault() {
        return metrics;
    }
    
    public static void initJmxReporter() {
        if(null == metrics) {
            JmxReporter.startDefault(metrics);
        }
    }
}
