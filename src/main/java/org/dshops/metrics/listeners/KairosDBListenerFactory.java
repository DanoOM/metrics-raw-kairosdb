package org.dshops.metrics.listeners;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricRegistry;

public class KairosDBListenerFactory {
    private static Map<String, EventListener> indexingListeners = new ConcurrentHashMap<>();
    private static Map<String, EventListener> unIndexingListeners = new ConcurrentHashMap<>();

    public static EventListener buildListener(String connectString, MetricRegistry registry){
        return buildListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildListener(String connectString,
                                              String un,
                                              String pd,
                                              MetricRegistry registry,
                                              int batchSize,
                                              int bufferSize,
                                              long offerTimeMillis) {
        EventListener listener = indexingListeners.get(connectString);
        if (listener == null) {
            synchronized (indexingListeners) {
                listener = indexingListeners.get(connectString);
                if (listener == null) {
                    listener = new KairosDbIndexingListener(connectString,
                                                            un,
                                                            pd,
                                                            registry);
                    indexingListeners.put(connectString, listener);
                }
            }
        }
        return listener;
    }

    public static EventListener buildUnindexedListener(String connectString, MetricRegistry registry){
        return buildUnindexedListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildUnindexedListener(String connectString,
                                              String un,
                                              String pd,
                                              MetricRegistry registry,
                                              int batchSize,
                                              int bufferSize,
                                              long offerTimeMillis) {
        EventListener listener = unIndexingListeners.get(connectString);
        if (listener == null) {
            synchronized (unIndexingListeners) {
                listener = unIndexingListeners.get(connectString);
                if (listener == null) {
                    listener = new KairosDbNonIndexingListener(connectString,
                                                               un,
                                                               pd,
                                                               registry);
                    unIndexingListeners.put(connectString, listener);
                }
            }
        }
        return listener;
    }
}
