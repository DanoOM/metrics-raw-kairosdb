package org.dshops.metrics.listeners;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricRegistry;

public class KairosDBListenerFactory {
    private static Map<String, EventListener> indexingListeners = new ConcurrentHashMap<>();
    private static Map<String, EventListener> unIndexingListeners = new ConcurrentHashMap<>();
    private static boolean enableListenerCaching = true;

    public static void enableListenerCaching(boolean enableCaching){
        enableListenerCaching = enableCaching;
    }

    public static boolean isListenerCachingEnabled() {
        return enableListenerCaching;
    }

    public static EventListener buildListener(String connectString, MetricRegistry registry) {
        return buildListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildListener(String connectString,
                                              String username,
                                              String password,
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
                                                            username,
                                                            password,
                                                            registry,
                                                            batchSize,
                                                            bufferSize,
                                                            offerTimeMillis);
                    if (enableListenerCaching) {
                        indexingListeners.put(connectString, listener);
                    }
                }
            }
        }
        return listener;
    }

    public static EventListener buildUnindexedListener(String connectString, MetricRegistry registry) {
        return buildUnindexedListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildUnindexedListener(String connectString,
                                                       String username,
                                                       String password,
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
                                                               username,
                                                               password,
                                                               registry,
                                                               batchSize,
                                                               bufferSize,
                                                               offerTimeMillis);
                    if(enableListenerCaching) {
                        unIndexingListeners.put(connectString, listener);
                    }
                }
            }
        }
        return listener;
    }
}
