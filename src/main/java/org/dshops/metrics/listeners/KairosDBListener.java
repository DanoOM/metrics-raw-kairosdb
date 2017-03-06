package org.dshops.metrics.listeners;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.dshops.metrics.DoubleEvent;
import org.dshops.metrics.Event;
import org.dshops.metrics.LongEvent;
import org.dshops.metrics.MetricKey;
import org.dshops.metrics.MetricRegistry;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notes: while all constructors take username/password, these are currently not used.
 *
 * Upload strategy:
 * All events will be uploaded in batch: batchSize (default: 100), or every 1 second, whichever comes first.
 *
 * */
public class KairosDBListener extends ThreadedListener implements Runnable {
    private final ConcurrentSkipListMap<Long,MetricTimeSlot> metricBuffer = new ConcurrentSkipListMap<>(); // time/
    private final int batchSize;
    private final long offerTime;   // amount of time we are willing to 'block' before adding an event to our buffer, prior to dropping it.
    private final HttpClient kairosDb;
    private final static Logger log = LoggerFactory.getLogger(KairosDBListener.class);
    private final MetricRegistry registry;
    private final AtomicInteger droppedEvents = new AtomicInteger();
    private final String serviceTeam;
    private final String app;
    private final String appType;
    private final int bufferLimit;
    private final AtomicInteger bufferedEvents = new AtomicInteger();
    private final Map<String,String> versions = new HashMap<>();

    KairosDBListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry) {
        this(connectString, un, pd, registry, 100);
    }

    KairosDBListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry,
                            int batchSize) {
        this(connectString, un, pd, registry, batchSize, 5000, -1);
    }
    KairosDBListener(HttpClient client,
            MetricRegistry registry) {
        this(client, registry, 100, 5000, -1);
    }
    KairosDBListener(HttpClient client,
                     MetricRegistry registry,
                     int batchSize,
                     int bufferSize,
                     long offerTimeMillis) {
        this.registry = registry;
        String[] prefix = registry.getPrefix().split("\\.");
        this.serviceTeam = prefix[0];
        this.app = prefix[1];
        this.appType = prefix[2];
        this.bufferLimit = bufferSize;

        if (batchSize > 1) {
            this.batchSize = batchSize;
        }
        else {
            this.batchSize = 100;
        }

        this.offerTime = offerTimeMillis;
        this.kairosDb = client;
        startThread(registry);
    }

    KairosDBListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry,
                            int batchSize,
                            int bufferSize,
                            long offerTimeMillis) {
        this.registry = registry;
        String[] prefix = registry.getPrefix().split("\\.");
        this.serviceTeam = prefix[0];
        this.app = prefix[1];
        this.appType = prefix[2];
        this.bufferLimit = bufferSize;

        if (batchSize > 1) {
            this.batchSize = batchSize;
        }
        else {
            this.batchSize = 100;
        }

        this.offerTime = offerTimeMillis;
        try {
            if (connectString != null) {
                // todo: no way configure timeout?
                kairosDb = new HttpClient(connectString);
            }
            else {
                log.warn("kairosDb Url is null - running in noop mode.");
                kairosDb = null;
            }
        }
        catch(MalformedURLException mue) {
            throw new RuntimeException("Malformed Url:"+connectString+" "+mue.getMessage());
        }
        startThread(registry);
    }

    private void startThread(MetricRegistry registry) {
        String kbListenerVersion = getVersion("org.dshops/metrics-raw-kairosdb", this.getClass());
        String metricsRawVersion = getVersion("org.dshops/metrics-raw", registry.getClass());

        System.out.println("kairosDbListener - Version Info[KairosDbListener:" + kbListenerVersion + ", metrics-raw:"+metricsRawVersion);

        if (kbListenerVersion != null) {
            versions.put("kairosDbListenerVersion", kbListenerVersion);
            versions.put("metricsRawVersion", metricsRawVersion);
        }

        runThread = new Thread(this);
        runThread.setName("kairosDbListener");
        runThread.setDaemon(true);
        runThread.start();
    }

    @Override
    public void run() {
        final List<IndexedEvent> dispatchList = new ArrayList<>(batchSize);
        long lastResetTime = System.currentTimeMillis();
        long httpCalls = 0;
        long metricCount = 0;
        long errorCount = 0;
        long exceptionTime = 0;
        Response lastError = null;
        do {
            try {
                addNewEvents(dispatchList);
                if(!dispatchList.isEmpty()) {
                    // @todo - consider bucketing..we may get multiple datapoints for the same ms, with the same name/tagSet, we will lose data with this approach atm.
                    metricCount += dispatchList.size();
                    if (kairosDb != null) {
                        Response r = kairosDb.pushMetrics(buildPayload(dispatchList));
                        httpCalls++;
                        if (r.getStatusCode() != 204 ) {
                            lastError = r;
                            errorCount++;
                        }
                    }
                    // Generate stat info
                    if (System.currentTimeMillis() - lastResetTime > 60_000) {
                        sendMetricStats(metricCount, errorCount, httpCalls);

                        if (lastError != null) {
                            StringBuilder sb = new StringBuilder();
                            for (String s : lastError.getErrors()) {
                                sb.append("[");
                                sb.append(s);
                                sb.append("]");
                            }
                            if (lastError != null) {
                                log.error("Http calls:{} Dispatch count: {} errorCount:{} lastError.status:{} lastErrorDetails:{}", httpCalls, metricCount, errorCount, lastError.getStatusCode(), sb.toString());
                            }
                            lastError = null;
                        }
                        droppedEvents.set(0);
                        httpCalls = 0;
                        errorCount = 0;
                        metricCount = 0;
                        lastResetTime = System.currentTimeMillis();
                    }
                }
            }
            catch(InterruptedException ie) {
                break;
            }
            catch(Exception ex) {
                errorCount++;
                if (System.currentTimeMillis() - exceptionTime > 60_000) {
                    log.error("Unexpected Exception (only 1 exception logged per minute)", ex);
                }
                exceptionTime = System.currentTimeMillis();
            }
            finally {
                dispatchList.clear();
            }
        } while(!stopRequested);
    }

    @Override
    public void stop() {
        super.stop();
        if(kairosDb != null) {
            try {
                kairosDb.shutdown();
            }
            catch (Exception e) {}
        }
    }

    // move events from the MetricBuffer to the dispatchList, will exist if over 1000 second of processing
    // or we have exceeded batchSize.
    // Any timeSlot started, will result in all events from that timeslot being added to dispatchList.
    private void addNewEvents(final List<IndexedEvent> dispatchList) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        do {
            Long ts = null;
            Map.Entry<Long,MetricTimeSlot> entry = metricBuffer.firstEntry();
            if (entry != null) {
                ts = entry.getKey();
                if (ts != null && ts < System.currentTimeMillis() - 50) {
                    MetricTimeSlot timeSlot = metricBuffer.remove(ts);
                    for (Map.Entry<MetricKey, ConcurrentLinkedQueue<Event>> metricEvents : timeSlot.metricMap.entrySet()) {
                        int i = 0;
                        for (Event event: metricEvents.getValue()) {
                            dispatchList.add(new IndexedEvent(event, i++));
                            bufferedEvents.decrementAndGet();
                        }
                    }
                    // send what we have if we have exceeded batchSize, or have gone over 1 second.
                    if (dispatchList.size() > batchSize || System.currentTimeMillis() - startTime > 1000) {
                        break;
                    }
                }
            }
            else {
                Thread.sleep(50);
            }
        } while(!stopRequested);
    }

    private void sendMetricStats(long metricCount, long errorCount, long httpCalls) throws Exception {
        if (kairosDb == null) return;
        try {
            MetricBuilder mb = MetricBuilder.getInstance();


            mb.addMetric("metricsraw.stats.data.count")
              .addTags(registry.getTags())
              .addTags(versions)
              .addTag("serviceTeam",serviceTeam)
              .addTag("app",app)
              .addTag("appType",appType)
              .addDataPoint(metricCount);
            mb.addMetric("metricsraw.stats.http.errors")
            .addTags(registry.getTags())
            .addTag("serviceTeam",serviceTeam)
            .addTag("app",app)
            .addTag("appType",appType)
              .addDataPoint(errorCount);
            mb.addMetric("metricsraw.stats.http.count")
            .addTags(registry.getTags())
            .addTag("serviceTeam",serviceTeam)
            .addTag("app",app)
            .addTag("appType",appType)
              .addDataPoint(httpCalls);
            mb.addMetric("metricsraw.stats.data.dropped")
            .addTags(registry.getTags())
            .addTag("serviceTeam",serviceTeam)
            .addTag("app",app)
            .addTag("appType",appType)
              .addDataPoint(droppedEvents.longValue());

             Response r = kairosDb.pushMetrics(mb);
             if (r.getStatusCode() != 204 ) {
                 log.warn("failed to send metric statistics!", r.getStatusCode());
             }
        }
        catch(Exception e) {
            log.warn("failed to send metric statistis to server! {} ", e.getMessage());
        }
    }

    private MetricBuilder buildPayload(List<IndexedEvent> events) {
        MetricBuilder mb = MetricBuilder.getInstance();
        for (IndexedEvent ie: events) {
            Event e = ie.event;
            if (e instanceof LongEvent) {
                mb.addMetric(e.getName())
                              .addTags(e.getTags())
                              .addTag("index", ie.index+"")
                              .addDataPoint(e.getTimestamp(), e.getLongValue());
            }
            else if (e instanceof DoubleEvent) {
                mb.addMetric(e.getName())
                              .addTags(e.getTags())
                              .addTag("index", ie.index+"")
                              .addDataPoint(e.getTimestamp(), e.getDoubleValue());
            }
            else {
                // this is a pure event, value has no meaning
                mb.addMetric(e.getName())
                              .addTags(e.getTags())
                              .addTag("index", ie.index+"")
                              .addDataPoint(e.getTimestamp(), 1);
            }
        }
        return mb;
    }

    @Override
    public int eventsBuffered() {
        return bufferedEvents.get();
    }

    @Override
    public void onEvent(Event e) {

        if (bufferedEvents.get() > bufferLimit) {
            if (offerTime > 0) {
                try {
                    long startTime = System.currentTimeMillis();
                    while (bufferedEvents.get() > bufferLimit && System.currentTimeMillis() - startTime < offerTime) {
                        Thread.sleep(10);
                    }
                    if (bufferedEvents.get() > bufferLimit){
                        droppedEvents.incrementAndGet();
                        return;
                    }
                }
                catch(InterruptedException ie) {
                    return;
                }
            }
            else {
                droppedEvents.incrementAndGet();
                return;
            }
        }

        MetricTimeSlot timeSlot = metricBuffer.get(e.getTimestamp());
        if (timeSlot == null) {
            timeSlot = new MetricTimeSlot();
            metricBuffer.put(e.getTimestamp(), timeSlot);
        }
        ConcurrentLinkedQueue<Event> events = timeSlot.metricMap.get(e.getHash());
        if (events == null) {
            events = new ConcurrentLinkedQueue<>();
            timeSlot.metricMap.put(e.getHash(), events);
            bufferedEvents.incrementAndGet();
        }
        events.add(e);
    }

    public synchronized String getVersion(String path, Class clazz) {
        String version = null;

        // try to load from maven properties first
        try {
            Properties p = new Properties();
            InputStream is = getClass().getResourceAsStream("/META-INF/maven/"+path+"/pom.properties");
            if (is != null) {
                p.load(is);
                version = p.getProperty("version", "");
            }
        }
        catch (Exception e) {
            // ignore
        }

        // fallback to using Java API
        if (version == null) {
            Package aPackage = clazz.getPackage();
            if (aPackage != null) {
                version = aPackage.getImplementationVersion();
                if (version == null) {
                    version = aPackage.getSpecificationVersion();
                }
            }
        }

        if (version == null) {
            // we could not compute the version so use a blank
            version = "";
        }

        return version;
    }
}

class IndexedEvent {
    final Event event;
    final int index;
    public IndexedEvent(Event e, int index){
        this.event = e;
        this.index = index;
    }
}

class MetricTimeSlot {
    Map<MetricKey,ConcurrentLinkedQueue<Event>> metricMap = new ConcurrentHashMap<>();
}