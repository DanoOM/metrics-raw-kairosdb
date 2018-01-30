package org.dshops.test.metrics.generators;

import java.util.Date;
import java.util.Random;

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
        MetricRegistry mr = new MetricRegistry.Builder("dshops", "metrics", "test", "testHost", "testDatacenter").build();
        mr.addEventListener(getListener(mr));

        long ts = System.currentTimeMillis() - 60_000;
        System.out.println(new Date(ts));
        mr.eventAtTs("testEvent-old", ts);
        mr.event("testEvent-now", ts);

        // timer used for 2 timeings.
        Timer reuseTimer = mr.getTimer("testManualTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        reuseTimer.start();
        // basic timer test
        Timer t = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        Timer t2 = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        Timer t3 = mr.timer("testTimer2", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        Timer notEnoughDataTimer = mr.percentileTimer("testNoDataPercentileTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        sleep(1000);
        reuseTimer.stop();
        reuseTimer.start();
        t.stop();
        t2.stop();
        // re-use' the first time.
        t = mr.timer("testTimer", "tag1", "tagValue1").addTag("tag2", "tagValue2");
        sleep(1000);
        reuseTimer.stop();
        t3.stop();
        t.stop();
        notEnoughDataTimer.stop(); // NOTE: will yield no result (since requires 100 samples)

        // Some Alerts (show up under <PREFIX>.alerts.
        mr.alert("testAlertNoValue");
        sleep(1000);
        mr.alert("testAlertWholeNumber", 100);
        sleep(1000);
        mr.alert("testAlertDoubleNumber", -100.555);
        sleep(1000);
        mr.alert("testAlertTagged","tag","tagValue");

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

        mr.scheduleGauge("testGauge", 1, () -> {
            return r.nextInt(100);
        }, "tag", "tagValue");
        // note cannot have 2 gauges with same name/tagset
        mr.scheduleGauge("testGauge", 5, () -> {
            return r.nextInt(100) + 200;
        }, "tag", "tagValue2");
        // note cannot have 2 gauges with same name/tagset
        mr.scheduleGauge("testGauge2", 1, () -> {
            return r.nextInt(100) + 400;
        }, "tag", "tagValue");


        Meter meter = mr.scheduleMeter("testMeter1s", 1);
        Meter meter2 = mr.scheduleMeter("testMeter5s", 5, "tag","tagvalue1");

        // counter test - 65 seconds

        for (int i = 0; i < 65_000; i++) {
            try {
                Timer tp = mr.percentileTimer("testPercentileTimer");
                Thread.sleep(r.nextInt(5));

                if (i % 100 == 0) { // this will give us a DataPoint every ~6 datapoints, our of 650 datapoints 'timed'..aka stopped()
                    tp.stop();
                }
                meter.mark();
                meter2.mark();
            }
            catch (Exception e) {
            }
            mr.counter("testCounter").increment();
        }
        System.out.println("Exiting");
    }

    public void indexingTest() {
        String url = "http://wdc-tst-masapp-001:8080";
        MetricRegistry mr = new MetricRegistry.Builder("dshops", "metrics", "test", "testHost", "testDatacenter").build();
        mr.addEventListener(getListener(mr));

        long startTime = System.currentTimeMillis();
        while(startTime == System.currentTimeMillis());
        mr.event("",1);
        mr.event("",2);
        mr.event("",3);
        mr.event("",4);
        mr.event("",5);
        mr.event("",6);
        mr.event("",7);
        mr.event("",8);
        mr.event("",9);
        mr.event("",10);
        System.out.println();
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
