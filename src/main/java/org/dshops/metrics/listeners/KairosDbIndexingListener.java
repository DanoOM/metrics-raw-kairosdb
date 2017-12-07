package org.dshops.metrics.listeners;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dshops.metrics.DoubleEvent;
import org.dshops.metrics.Event;
import org.dshops.metrics.EventIndexingListener;
import org.dshops.metrics.LongEvent;
import org.dshops.metrics.MetricRegistry;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Will be moved library metric-raw-kairosdb in the future.
 * Notes: while all constructors take username/password, these are currently not used.
 *
 * Upload strategy:
 * All events will be uploaded in batch: batchSize (default: 100), or every 1 second, whichever comes first.
 * A status update will be logged at level info, once every 5 minutes, indicating the number of the number http calls (dispatchCount), as well
 * the number of metric datapoints, the number or errors (with details on the last error that occured).
 *
 *
 * */
public class KairosDbIndexingListener extends ThreadedListener
implements Runnable, EventIndexingListener {    
    private final BlockingQueue<Event> queue;
    private final int batchSize;
    private final long offerTime;   // amount of time we are willing to 'block' before adding an event to our buffer, prior to dropping it.
    private Thread runThread;
    private final HttpClient kairosDb;
    private final static Logger log = LoggerFactory.getLogger(KairosDbIndexingListener.class);
    private final MetricRegistry registry;
    private final AtomicInteger droppedEvents = new AtomicInteger();
    private final String namespace;
    private final String app;
    private final String appType;
    private final Map<String,String> versions = new HashMap<>();
    private final ExecutorService executor;

    KairosDbIndexingListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry) {
        this(connectString, un, pd, registry, 100);
    }

    KairosDbIndexingListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry,
                            int batchSize) {
        this(connectString, un, pd, registry, batchSize, 5000, -1, 2);
    }

    KairosDbIndexingListener(String connectString,
                            String un,
                            String pd,
                            MetricRegistry registry,
                            int batchSize,
                            int bufferSize,
                            long offerTimeMillis,
                            int maxDispatchThreads) {        
    	this.registry = registry;
    	if (registry !=null) {
        	String[] prefix = registry.getPrefix().split("\\.");
        	this.namespace = prefix[0];
        	this.app = prefix[1];
        	this.appType = prefix[2];
    	}
    	else {
    	    this.namespace = null;
    	    this.app = null;
    	    this.appType = null;
    	}

        this.queue = new ArrayBlockingQueue<>(bufferSize);
        if (batchSize > 1) {
            this.batchSize = batchSize;
        }
        else {
            this.batchSize = 100;
        }

        this.offerTime = offerTimeMillis;
        try {
            RequestConfig reqConfig = RequestConfig.custom()
                                                   .setStaleConnectionCheckEnabled(false)
                                                   .setConnectTimeout(5000)
                                                   .setSocketTimeout(5000)
                                                   .setConnectionRequestTimeout(5000)
                                                   .build();
            
            HttpClientBuilder builder = HttpClientBuilder.create();
            builder.setMaxConnPerRoute(maxDispatchThreads)
                   .setDefaultRequestConfig(reqConfig);
            kairosDb = new HttpClient(builder, connectString);
            
        }
        catch(MalformedURLException mue) {
            throw new RuntimeException("Malformed Url:"+connectString+" "+mue.getMessage());
        }

        // Get Version Info
        String kbListenerVersion = getVersion("org.dshops/metrics-raw-kairosdb", this.getClass());
        if (registry != null) {
            String metricsRawVersion = getVersion("org.dshops/metrics-raw", registry.getClass());
            System.out.println("kairosDbListener - Version Info[KairosDbListener:" + kbListenerVersion + ", metrics-raw:"+metricsRawVersion);
            if (kbListenerVersion != null) {
                versions.put("kairosDbListenerVersion", kbListenerVersion);
                versions.put("metricsRawVersion", metricsRawVersion);
            }
        }
        
        executor = new ThreadPoolExecutor(Math.min(1,maxDispatchThreads),                    // CORE
                                          maxDispatchThreads,   // max thread growth
                                          10l,                  // keepAlive
                                          TimeUnit.SECONDS,
                                          new LinkedBlockingQueue<>(10),                                          
                                          new ThreadPoolExecutor.CallerRunsPolicy());

        runThread = new Thread(this);
        runThread.setName("KairosDbIndexingListener");
        runThread.setDaemon(true);
        runThread.start();
    }

    HttpClient getClient() {
        return kairosDb;
    }

    @Override
    public void run() {
        List<Event> dispatchList = new ArrayList<>(batchSize);
        long lastResetTime = System.currentTimeMillis();
        long httpCalls = 0;
        long metricCount = 0;
        long errorCount = 0;
        long exceptionTime = 0;
        Response lastError = null;
        List<Future<Response>> results = new LinkedList<>();
        do {
            try {
                // block until we have at least 1 metric
                dispatchList.add(queue.take());

                // try to always send a minimum of dispatchSize datapoints per call.
                long takeTime = System.currentTimeMillis();
                do {
                    Event e = queue.poll(10, TimeUnit.MILLISECONDS);
                    if (e != null) {
                        dispatchList.add(e);
                    }
                    // flush every second or until we have seen batchSize Events
                } while(dispatchList.size() < batchSize && (System.currentTimeMillis() - takeTime < 500));

                // @todo - consider bucketing..we may get multiple datapoints for the same ms, with the same name/tagSet, we will lose data with this approach atm.
                
                DataDispatcher dd = new DataDispatcher(kairosDb, dispatchList);                
                results.add(executor.submit(dd));
                metricCount += dispatchList.size();              
                httpCalls++;                
                Iterator<Future<Response>> itr = results.iterator();
                while (itr.hasNext()) {
                    try {
                        Future<Response> f = itr.next();
                        if (f.isDone()) {
                            Response r = f.get();
                            itr.remove();
                            if (r.getStatusCode() != 204 ) {
                                lastError = r;
                                errorCount++;
                            }
                        }
                    }
                    catch(Exception e) {
                        log.error("Exception processing future!", e);                            
                        try{itr.remove();} catch(Exception ex){}
                    }
                }
                
                // every 1 minute report general statistics
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
            catch(InterruptedException ie) {
                break;
            }
            catch(Exception ex) {
                errorCount++;
                if (System.currentTimeMillis() - exceptionTime > 60_000) {
                    log.warn("Unexpected Exception (only 1 exception logged per minute)", ex);
                    exceptionTime = System.currentTimeMillis();
                }
            }
            finally {
                if (!dispatchList.isEmpty())            
                    dispatchList = new ArrayList<>(batchSize);
            }
        } while(true);
    }

    private void sendMetricStats(long metricCount, long errorCount, long httpCalls) throws Exception {
    	try {

    	    if (registry == null) return;

	    	MetricBuilder mb = MetricBuilder.getInstance();
	    	mb.addMetric("metricsraw.stats.data.count")
	          .addTags(versions)
	    	  .addTags(registry.getTags())
	    	  .addDataPoint(metricCount);
	    	mb.addMetric("metricsraw.stats.http.errors")
	    	  .addTags(registry.getTags())
	    	  .addDataPoint(errorCount);
	    	mb.addMetric("metricsraw.stats.http.count")
	    	  .addTags(registry.getTags())
	    	  .addDataPoint(httpCalls);
	    	mb.addMetric("metricsraw.stats.data.dropped")
	    	  .addTags(registry.getTags())
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

    private MetricBuilder buildPayload(List<Event> events) {
        MetricBuilder mb = MetricBuilder.getInstance();
        for (Event e: events) {
            if (e.getIndex() > 1) {
                if (e instanceof LongEvent) {
                    mb.addMetric(e.getName())
                       .addTags(e.getTags())
                       .addTag("index", e.getIndex() + "")
                       .addDataPoint(e.getTimestamp(), e.getLongValue());
                }
                else if (e instanceof DoubleEvent) {
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", e.getIndex() + "")
                      .addDataPoint(e.getTimestamp(), e.getDoubleValue());
                }
                else {
                    // this is a pure event, value has no meaning
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", e.getIndex() + "")
                      .addDataPoint(e.getTimestamp(), 1);
                }
            }
            else {
                if (e instanceof LongEvent) {
                    mb.addMetric(e.getName())
                       .addTags(e.getTags())
                       .addTag("index", "1")
                       .addDataPoint(e.getTimestamp(), e.getLongValue());
                }
                else if (e instanceof DoubleEvent) {
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", "1")
                      .addDataPoint(e.getTimestamp(), e.getDoubleValue());
                }
                else {
                    // this is a pure event, value has no meaning
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", "1")
                      .addDataPoint(e.getTimestamp(), 1);
                }
            }
        }
        return mb;
    }

    @Override
    public int eventsBuffered() {
        return queue.size();
    }

    @Override
    public void onEvent(Event e) {
        if (offerTime > 0) {
            try {
                if (!queue.offer(e, offerTime, TimeUnit.MILLISECONDS)) {
                	droppedEvents.incrementAndGet();
                }
            }
            catch(InterruptedException ie) {
                // swallow
            }
        }
        else {
            if (!queue.offer(e)) {
                droppedEvents.incrementAndGet();
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        if(kairosDb != null) {
            try {
                executor.shutdown();
                kairosDb.shutdown();
            }
            catch (Exception e) {}
        }
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

class DataDispatcher implements Callable<Response> {
    private final HttpClient kairosClient;
    private final List<Event> events;
    
    public DataDispatcher(HttpClient kairosClient, List<Event> events) {
        this.kairosClient = kairosClient;
        this.events = events;
    }

    @Override
    public Response call() throws Exception {
        Response r = kairosClient.pushMetrics(buildPayload(events));
        return r;
    }
    
    private static MetricBuilder buildPayload(List<Event> events) {
        MetricBuilder mb = MetricBuilder.getInstance();
        for (Event e: events) {
            if (e.getIndex() > 1) {
                if (e instanceof LongEvent) {
                    mb.addMetric(e.getName())
                       .addTags(e.getTags())
                       .addTag("index", e.getIndex() + "")
                       .addDataPoint(e.getTimestamp(), e.getLongValue());
                }
                else if (e instanceof DoubleEvent) {
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", e.getIndex() + "")
                      .addDataPoint(e.getTimestamp(), e.getDoubleValue());
                }
                else {
                    // this is a pure event, value has no meaning
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", e.getIndex() + "")
                      .addDataPoint(e.getTimestamp(), 1);
                }
            }
            else {
                if (e instanceof LongEvent) {
                    mb.addMetric(e.getName())
                       .addTags(e.getTags())
                       .addTag("index", "1")
                       .addDataPoint(e.getTimestamp(), e.getLongValue());
                }
                else if (e instanceof DoubleEvent) {
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", "1")
                      .addDataPoint(e.getTimestamp(), e.getDoubleValue());
                }
                else {
                    // this is a pure event, value has no meaning
                    mb.addMetric(e.getName())
                      .addTags(e.getTags())
                      .addTag("index", "1")
                      .addDataPoint(e.getTimestamp(), 1);
                }
            }
        }
        return mb;
    }
}