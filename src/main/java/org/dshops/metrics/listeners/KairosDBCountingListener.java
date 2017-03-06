package org.dshops.metrics.listeners;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.dshops.metrics.Event;
import org.dshops.metrics.EventListener;
import org.dshops.metrics.MetricKey;

/**
 * Notes: while all constructors take username/password, these are currently not used.
 *
 * Upload strategy:
 * All events will be uploaded in batch: batchSize (default: 100), or every 1 second, whichever comes first.
 *
 * */
public class KairosDBCountingListener extends ThreadedListener implements Runnable {
    private EventListener listener;
    private Map<MetricKey,ResetCounter> counts = new ConcurrentHashMap<>();

    KairosDBCountingListener(EventListener listener) {
        this.listener = listener;
    }

    @Override
    public void onEvent(Event e) {
        ResetCounter counter = counts.get(e.getHash());
        if (counter == null) {
            synchronized (counts) {
                counter = counts.get(e.getHash());
                if (counter == null) {
                    counter = new ResetCounter();
                    counts.put(e.getHash(),counter);
                }
            }
        }
        try {
            e.setIndex(counter.getCount());
        }catch(Exception ex){
            ex.printStackTrace();
        }
        listener.onEvent(e);
    }

    @Override
    public int eventsBuffered() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }
}

// option 1 (no gc overhead/atomic/sync)
class ResetCounter {
    public AtomicInteger counter = new AtomicInteger();
    public long ts;
    public int getCount() {
        int count = counter.incrementAndGet();
        if (System.currentTimeMillis() - ts  > 1) {
            synchronized (this) {
                ts = System.currentTimeMillis();
                counter.set(0);
            }
        }
        return count;
    }
}

//option 2 (gc overhead), no atomics
class ResetCounter2 {
 public LongAdder counter;
 public long ts;

 public int getCount() {
     counter.increment();
     int count = counter.intValue();
     if (System.currentTimeMillis() - ts  > 1) {
         ts = System.currentTimeMillis();
         counter = new LongAdder();
     }
     return count;
 }
}