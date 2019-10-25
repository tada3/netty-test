package netty.test.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BatchLogger {

    private Queue<LogEvent> queue = new ConcurrentLinkedQueue<>();
    private LogAggregator agg = new LogAggregator();
    private ExecutorService es = Executors.newSingleThreadExecutor();
    private static final LogEvent FIN = new LogEvent(-1, -1, -1);

    public BatchLogger() {
        es.submit(() -> {
            while (true) {
                try {
                    LogEvent e = queue.poll();
                    if (e == null) {
                        Thread.sleep(100);
                        continue;
                    }
                    if (e == FIN) {
                        break;
                    }
                    long tp = agg.add(e);
                    if (tp >= 0) {
                        Util.print("TP: %d", tp);
                    }
                } catch (Exception e) {
                    Util.handleError(e);
                    break;
                }
            }
        });

    }

    public void log(long from, long count) {
        long now = System.currentTimeMillis();
        LogEvent event = new LogEvent(from, now, count);
        queue.offer(event);
    }

    public LogEvent get() {
        return queue.poll();
    }

    public LogAggregator getAggregator() {
        return agg;
    }

    public void close() {
        queue.offer(FIN);
        es.shutdown();
        try {
            es.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Util.handleError(ie);
            Thread.currentThread().interrupt();
        } finally {
            es.shutdownNow();
        }
    }

    public static class LogEvent {
        public long from;
        public long to;
        public long count;

        public LogEvent(long f, long t, long c) {
            from = f;
            to = t;
            count = c;
        }
    }

    public static class LogAggregator {
        //private static final long THRESHOLD = 100000;
        private static final long THRESHOLD = 10000;

        private long start = Long.MAX_VALUE;
        private long end = 0;
        private long total = 0;

        public long add(LogEvent e) {

            if (e.from < start) {
                start = e.from;
            }
            if (e.to > end) {
                end = e.to;
            }
            total += e.count;

            if (total >= THRESHOLD) {
                return getThroughputAndReset();
            }
            return -1;
        }

        private void reset() {
            start = Long.MAX_VALUE;
            end = 0;
            total = 0;
        }

        // query / sec
        private long getThroughputAndReset() {
            long tp = total / ((end - start) / 1000L);
            reset();
            return tp;
        }
    }

}
