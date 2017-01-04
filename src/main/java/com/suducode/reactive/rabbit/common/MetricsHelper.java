package com.suducode.reactive.rabbit.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import java.util.Optional;

/**
 * Class that helps register and manage metrics
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class MetricsHelper {

    private static MetricsHelper metricsHelper;

    private static final String BASE = "com.suducode";

    private MetricRegistry metricRegistry = new MetricRegistry();
    private final JmxReporter reporter;

    private MetricsHelper() {
        reporter = JmxReporter.forRegistry(metricRegistry).build();
        reporter.start();
    }

    public static synchronized MetricsHelper getInstance() {
        if (metricsHelper == null) {
            metricsHelper = new MetricsHelper();
        }
        return metricsHelper;
    }


    /**
     * helps register a metric
     *
     * @param scope  of the metric to identify itself
     * @param metric the actual metric
     */
    private void registerMetric(String scope, Metric metric) {
        metricRegistry.register(scope, metric);
    }

    public Counter getCounter(String id, String scope, String name) {
        Counter counter = metricRegistry.counter(getName(id, scope, name));
        if (!Optional.ofNullable(counter).isPresent()) {
            counter = new Counter();
            registerMetric(scope, counter);
        }
        return counter;
    }

    private String getName(String id, String scope, String name) {
        return MetricRegistry.name(BASE, id, scope, name);
    }

}
