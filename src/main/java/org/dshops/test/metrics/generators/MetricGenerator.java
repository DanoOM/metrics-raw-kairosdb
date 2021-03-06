package org.dshops.test.metrics.generators;

import static org.dshops.test.metrics.generators.UtilArg.getArg;
import static org.dshops.test.metrics.generators.UtilArg.getIntArg;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricRegistry;
import org.dshops.metrics.listeners.KairosDBListenerFactory;
import org.kairosdb.client.HttpClient;

/** Metric Generators is used for 'testing', aka generating metrics
 *  This generator is specific to KairosDB., and will be moved library metric-raw-kairosdb in the future.
 * */
public class MetricGenerator {

    public static void main(String[] args) {
        // Disable caching, as we create unique MR/Listener for 'simulated' host.
        MetricRegistry.enableRegistryCaching(false);
        KairosDBListenerFactory.enableListenerCaching(false);
        new MetricGenerator(args);
    }

    public EventListener getListener(MetricRegistry reg, String url) {
        return KairosDBListenerFactory.buildListener(url,
                                                    "root",
                                                    "root",
                                                    reg,
                                                    500,
                                                    12_000,
                                                    -1);
    }

    public MetricGenerator(String[] args) {
        try {
            String url = getArg(args, "url", "http://wdc-tst-masapp-003:8080");
            String user = getArg(args, "u", "root");
            String pd = getArg(args, "p", "root");

            String service = getArg(args, "s", "dshops");
            String app = getArg(args, "a", "metrics");
            String appType = getArg(args, "T", "testload");

            // simulations
            final long runTime = getIntArg(args, "t", Integer.MAX_VALUE); // runtime minutes
            int maxEvents = getIntArg(args,"m", Integer.MAX_VALUE);       // maximum number of events to generate )-1 unlimited, test will exist at the time limit.
            final int hosts = getIntArg(args, "h", 1);                    // unique host tag names: host<#>   (1 thread per host ( 1 MR per host)

            // values below are 'per-thread'
            final int eventSignatures = getIntArg(args, "e", 1);    // unique Event Names/thread        (event<#>)
            final int tagCount = getIntArg(args, "tagCount", 1);    // even event generated will have a random number of tags (upto this number)
            final int tagValues = getIntArg(args, "tagValues", 5);  // random value per tag
            final int writeTps = getIntArg(args, "tps", 500);       // tps target per host
            final int querytps = getIntArg(args, "qtps", 0);        // tps target per host

            EventGenerator[] writers = new EventGenerator[hosts];
            AtomicInteger dpWrittenCount = new AtomicInteger();
            AtomicInteger queryCounter = new AtomicInteger();
            AtomicInteger dataPointsRead = new AtomicInteger();
            AtomicBoolean exitFlag = new AtomicBoolean();
            System.out.println("SETTINGS");
            System.out.println("url                 " + url);
            System.out.println("service             " + service);
            System.out.println("app                 " + app);
            System.out.println("app                 " + appType);
            System.out.println("runtime             " + runTime);
            System.out.println("eventNames/thread:  " + eventSignatures);
            System.out.println("tags/thread:        " + tagCount);
            System.out.println("values/thread:      " + tagValues);
            System.out.println("TPS/thread:         " + writeTps);
            System.out.println("Query-TPS/thread:   " + querytps);

            String hostPrefix = InetAddress.getLocalHost().getHostName();

            for (int i = 0; i < hosts; i++) {
                String hostname = hostPrefix +i;
                MetricRegistry mr = new MetricRegistry.Builder(service, app, appType, hostname, "datacenter1").build();

                if (writeTps > 0) {
                    writers[i] = new EventGenerator(hostname,
                                                eventSignatures,
                                                exitFlag,
                                                dpWrittenCount,
                                                mr,
                                                tagCount,
                                                tagValues,
                                                writeTps);
                }

                String[] eventNames = new String[eventSignatures];
                for (int j = 0 ; j < eventNames.length;j++){
                    eventNames[j] = "event"+j;
                }

                String[] tagVals = new String[tagValues];
                for (int k = 0; k < tagVals.length; k++){
                    tagVals[k] = "value"+k;
                }

                Map<String,String[]> tags = new HashMap<>();
                for (int j = 0; j < tagCount; j++) {
                    tags.put("tag"+j, tagVals);
                }
                HttpClient kairosDb = new HttpClient(url);
                EventQueryGenerator eqg = new EventQueryGenerator(kairosDb,
                                                                  eventNames,
                                                                  tags,
                                                                  querytps,
                                                                  exitFlag,
                                                                  service+"."+app, // prefix
                                                                  queryCounter,
                                                                  dataPointsRead);
                eqg.start();

                if (writeTps > 0) {
                    EventListener listener = getListener(mr, url);
                    mr.addEventListener(listener);
                    writers[i].start();
                }
            }
            long endTime = System.currentTimeMillis() + (runTime * 60_000);
            long reportTime = System.currentTimeMillis() + 1_000;
            long generatedEvents = 0;
            long queries = 0;
            while (System.currentTimeMillis() < endTime && dpWrittenCount.get() < maxEvents) {
                Thread.sleep(1000); // 10 seconds
                if (System.currentTimeMillis() > reportTime) {
                    long remainingTime = endTime - System.currentTimeMillis();

                    long newEvents = dpWrittenCount.get() - generatedEvents;
                    float writtenTps = newEvents / 10;
                    generatedEvents += newEvents;

                    long newQueries = queryCounter.get() - queries;
                    float qtps = newQueries / 10;
                    queries += newQueries;
                    System.out.println("-->WRITES--[dataPoints:" + dpWrittenCount.get() + " TPS:" +writtenTps+ "] READS--[queries:"+queryCounter+" QPS:"+qtps+" dataPoints:"+dataPointsRead+"]  Time Remaining (sec): " + (remainingTime/1000));
                    reportTime = System.currentTimeMillis() + 10_000;
                }
            }
            exitFlag.set(true);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }


}

