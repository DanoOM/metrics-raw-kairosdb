package org.dshops.metrics.listeners;

import java.util.HashSet;
import java.util.Set;

import org.dshops.metrics.Event;
import org.dshops.metrics.MetricRegistry;

public class KairosDbHybridListener extends ThreadedListener {

    private final KairosDbListenerMilliBucket bucketListener;
    private final KairosDBListener indexedListener;
    private Set<String> indexedMetrics = new HashSet<>();
    private final MetricRegistry registry;

    KairosDbHybridListener(String connectString, MetricRegistry registry) {
        this.registry = registry;
        bucketListener = new KairosDbListenerMilliBucket(connectString, "", "", registry);
        indexedListener = new KairosDBListener(bucketListener.getClient(), registry);
    }

    public void setIndexedMetrics(Set<String> metricNames) {
        indexedMetrics.clear();
        metricNames.stream()
                    .forEach( metricName ->
                        indexedMetrics.add(registry.getPrefix() + metricName));
    }

    @Override
    public void onEvent(Event e) {
        if (indexedMetrics.contains(e.getName())){
            indexedListener.onEvent(e);
        }
        else {
            bucketListener.onEvent(e);
        }
    }

    @Override
    public int eventsBuffered() {
        return bucketListener.eventsBuffered() + indexedListener.eventsBuffered();
    }

    @Override
    public void stop() {
        bucketListener.stop();
        indexedListener.stop();
    }

}
