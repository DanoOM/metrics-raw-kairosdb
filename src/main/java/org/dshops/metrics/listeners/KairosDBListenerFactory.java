package org.dshops.metrics.listeners;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricRegistry;

public class KairosDBListenerFactory {
    private static Map<String, EventListener> indexedListeners = new ConcurrentHashMap<>();
    private static Map<String, EventListener> bucketListeners = new ConcurrentHashMap<>();
    private static Map<String, KairosDbHybridListener> hybridListeners = new ConcurrentHashMap<>();
    private static Map<String, EventListener> countingListeners = new ConcurrentHashMap<>();

    /** Builds a 'default' KairosDbListener (non-indexed)
     *  where: 'un/pd='', batchSize = 50, bufferSize=5000, offerTimeMillis (none)'
     * */
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
        EventListener listener = bucketListeners.get(connectString);
        if (listener == null) {
            synchronized (bucketListeners) {
                listener = bucketListeners.get(connectString);
                if (listener == null) {
                    listener = new KairosDbListenerMilliBucket(connectString,
                                                               un,
                                                               pd,
                                                               registry);
                    bucketListeners.put(connectString, listener);
                }
            }
        }
        return listener;
    }

    /** Builds a 'default' KairosDbListener (Indexed)
     *  where: 'un/pd='', batchSize = 50, bufferSize=5000, offerTimeMillis (none)'
     * */
    public static EventListener buildIndexedListener(String connectString, MetricRegistry registry){
        return buildListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildIndexedListener(String connectString,
                                              String un,
                                              String pd,
                                              MetricRegistry registry,
                                              int batchSize,
                                              int bufferSize,
                                              long offerTimeMillis) {
        EventListener listener = indexedListeners.get(connectString);
        if (listener == null) {
            synchronized (indexedListeners) {
                listener = indexedListeners.get(connectString);
                if (listener == null) {
                    listener = new KairosDBListener(connectString,
                                                    un,
                                                    pd,
                                                    registry);
                    indexedListeners.put(connectString, listener);
                }
            }
        }
        return listener;
    }

    public static KairosDbHybridListener buildHybridListener(String connectString, MetricRegistry registry){
        return buildHybridListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static KairosDbHybridListener buildHybridListener(String connectString,
                                              String un,
                                              String pd,
                                              MetricRegistry registry,
                                              int batchSize,
                                              int bufferSize,
                                              long offerTimeMillis) {
        KairosDbHybridListener listener = hybridListeners.get(connectString);
        if (listener == null) {
            synchronized (hybridListeners) {
                listener = hybridListeners.get(connectString);
                if (listener == null) {
                    listener = new KairosDbHybridListener(connectString,
                                                          registry);
                    hybridListeners.put(connectString,listener);
                }
            }
        }
        return listener;
    }

    public static EventListener buildCountingListener(String connectString, MetricRegistry registry){
        return buildCountingListener(connectString, "", "", registry, 50, 5000, -1);
    }

    public static EventListener buildCountingListener(String connectString,
                                                      String un,
                                                      String pd,
                                                      MetricRegistry registry,
                                                      int batchSize,
                                                      int bufferSize,
                                                      long offerTimeMillis) {
        EventListener listener = countingListeners.get(connectString);
        if (listener == null) {
            synchronized (countingListeners) {
                listener = countingListeners.get(connectString);
                if (listener == null) {
                    EventListener el = new KairosDbListenerMilliBucket(connectString,
                                                                       un,
                                                                       pd,
                                                                       registry,
                                                                       batchSize,
                                                                       bufferSize,
                                                                       offerTimeMillis);
                    listener = new KairosDBCountingListener(el);
                    countingListeners.put(connectString, listener);
                }
            }
        }
        return listener;
    }
}
