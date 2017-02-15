package org.dshops.test.generators;

import org.dshops.listeners.KairosDBListener;
import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricRegistry;

public class KairosDbJvmMetricsTest extends JvmMetricsGenerator {

    public static void main(String args[]) {
        new KairosDbJvmMetricsTest().testJvmMetrics();
    }

    @Override
    public EventListener getListener(MetricRegistry reg) {
        return new KairosDBListener("http://wdc-tst-masapp-002:8080",
                "root",
                "root",
                reg,
                100);
    }
}
