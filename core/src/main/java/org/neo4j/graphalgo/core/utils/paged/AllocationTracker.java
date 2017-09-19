package org.neo4j.graphalgo.core.utils.paged;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class AllocationTracker implements Supplier<String> {
    public static final AllocationTracker EMPTY = new AllocationTracker() {
        @Override
        public void add(long delta) {
        }

        @Override
        public void remove(long delta) {
        }

        @Override
        public long tracked() {
            return 0L;
        }

        @Override
        public String get() {
            return "";
        }

        @Override
        public String getUsageString() {
            return "";
        }

        @Override
        public String getUsageString(String label) {
            return "";
        }
    };

    private static final String[] UNITS = new String[]{" Bytes", " KiB", " MiB", " GiB", " TiB", " PiB", "EiB", "ZiB", "YiB"};

    private final AtomicLong count = new AtomicLong();

    public void add(long delta) {
        count.addAndGet(delta);
    }

    public void remove(long delta) {
        count.addAndGet(-delta);
    }

    public long tracked() {
        return count.get();
    }

    public String getUsageString() {
        return humanReadable(tracked());
    }

    public String getUsageString(String label) {
        return label + humanReadable(tracked());
    }

    @Override
    public String get() {
        return getUsageString("Memory usage: ");
    }

    public static AllocationTracker create() {
        return new AllocationTracker();
    }

    public static boolean isTracking(AllocationTracker tracker) {
        return tracker != null && tracker != EMPTY;
    }

    /**
     * Returns <code>size</code> in human-readable units.
     */
    public static String humanReadable(long bytes) {
        for (String unit : UNITS) {
            // allow for a bit of overflow before going to the next unit to
            // show a diff between, say, 1.1 and 1.2 MiB as 1150 KiB vs 1250 KiB
            if (bytes >> 13 == 0) {
                return Long.toString(bytes) + unit;
            }
            bytes = bytes >> 10;
        }
        // we can never arrive here, longs are not large enough to
        // represent > 8192 yobibytes
        return null;
    }
}
