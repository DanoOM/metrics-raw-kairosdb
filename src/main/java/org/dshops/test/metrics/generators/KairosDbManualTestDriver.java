package org.dshops.test.metrics.generators;

import java.util.Random;

import org.dshops.metrics.BucketMetricRegistry;
import org.dshops.metrics.EventListener;
import org.dshops.metrics.Meter;
import org.dshops.metrics.MetricRegistry;
import org.dshops.metrics.Timer;
import org.dshops.metrics.listeners.KairosDBListenerFactory;

public class KairosDbManualTestDriver {

    private String url;
    public EventListener getListener(MetricRegistry reg) {
        return KairosDBListenerFactory.buildListener(url, reg);
    }

    public KairosDbManualTestDriver(String[] args) {
        url = UtilArg.getArg(args, "url", "http://wdc-tst-masapp-001:8080");
        MetricRegistry mr = new BucketMetricRegistry.Builder("dshops", "metrics", "test", "testHost", "testDatacenter").build();
        mr.addEventListener(getListener(mr));
        // basic timer test
        Timer t = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        Timer t2 = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        Timer t3 = mr.timer("testTimer2", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        sleep(1000);
        t.stop();
        t2.stop();
        // re-use' the first time.
        t = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        sleep(1000);
        t3.stop();
        t.stop();




        // Basic event test with value
        mr.event("testEventWholeNumber", 10);
        mr.event("testEventWholeNumber", 20);
        mr.event("testEventWholeNumber", 30);
        mr.event("testEventWholeNumber", 40);
        mr.event("testEventWholeNumber", 50);
        mr.event("testEventDouble", 10.6);
        mr.event("testEventDouble", 10.7);
        mr.event("testEventDouble", 10.8);
        mr.event("testEventDouble", 10.9);
        mr.event("testEventDouble", 11.0);

        mr.event("testEventWholeNumber", 10, "tag", "tagValue");
        mr.event("testEventEventDouble", 10.6, "tag", "tagValue");
        // Gauge test
        Random r = new Random();

        mr.scheduleGauge("testGauage", 1, () -> {
            return r.nextInt(100);
        }, "tag", "tagValue");
        // note cannot have 2 gauges with same name/tagset
        mr.scheduleGauge("testGauage", 5, () -> {
            return r.nextInt(100) + 200;
        }, "tag", "tagValue2");
        // note cannot have 2 gauges with same name/tagset
        mr.scheduleGauge("testGauage2", 1, () -> {
            return r.nextInt(100) + 400;
        }, "tag", "tagValue");


        Meter meter = mr.scheduleMeter("testRateGauge1s", 1);
        Meter meter2 = mr.scheduleMeter("testRateGauge5s", 5, "tag","tagvalue1");

        // counter test - 65 seconds
        for (int i = 0; i < 65_000; i++) {
            try {
                Thread.sleep(r.nextInt(5));
                meter.mark();
                meter2.mark();
            }
            catch (Exception e) {
            }
            mr.counter("testCounter").increment();
        }
        System.out.println("Exiting");

    }

    public static void main(String[] args) {
        new KairosDbManualTestDriver(args);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {

        }
    }

}
