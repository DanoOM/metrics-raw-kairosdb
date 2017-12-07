# metrics-raw-kairosdb
Implements the EventListener Interface defined in metrics-raw.  Sends metric-event data to KairosDB.

# Usage

Creating a KairosDBListener via KairosDBListenerFactory:
1.  buildListener(String connectString, MetricRegistry registry)
2. public static EventListener buildListener(String connectString,
                                             String username,
                                             String password,
                                             MetricRegistry registry,
                                             int batchSize,
                                             int bufferSize,
                                             long offerTimeMillis,
                                             int maxDispatchThreads)

Where:
 1. batchSize = max events sent to the kairosdb per http request.
 2. bufferSize = max internal buffer
 3. maxDispatchThreads = max number of concurrent httpRequests/threads for sending events to kairosdb.



