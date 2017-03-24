package org.neo4j.graphalgo;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.sun.management.GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

final class GcLogger implements NotificationListener, NotificationFilter, GcRunner {
    private static final long serialVersionUID = 133742;
    private static final String[] PREFIXES = new String[]{"B ", "KB", "MB", "GB"};

    static GcRunner install() {
        final Semaphore semaphore = new Semaphore(2);
        final List<MemoryPoolMXBean> memoryPools = ManagementFactory.getMemoryPoolMXBeans();
        final Collection<String> heapPools = new ArrayList<>();
        for (MemoryPoolMXBean memoryPool : memoryPools) {
            if (memoryPool.getType() == MemoryType.HEAP) {
                heapPools.add(memoryPool.getName());
            }
        }
        final GcLogger logger = new GcLogger(heapPools, semaphore);
        for (final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gcBean instanceof NotificationBroadcaster) {
                ((NotificationBroadcaster) gcBean).addNotificationListener(
                        logger,
                        logger,
                        null);
            }
        }
        return logger;
    }

    private final Collection<String> heapPools;
    private final Semaphore semaphore;

    private GcLogger(
            final Collection<String> heapPools,
            final Semaphore semaphore) {
        this.heapPools = heapPools;
        this.semaphore = semaphore;
    }

    @Override
    public boolean isNotificationEnabled(final Notification notification) {
        return notification
                .getType()
                .equals(GARBAGE_COLLECTION_NOTIFICATION);
    }

    @Override
    public void handleNotification(
            final Notification notification,
            final Object handback) {
        final GarbageCollectionNotificationInfo collectionInfo = obtainGcInfo(
                notification);
        final GcInfo info = collectionInfo.getGcInfo();

        final StringBuilder sb = new StringBuilder();
        print(
                "before",
                filterMap(info.getMemoryUsageBeforeGc()),
                collectionInfo,
                sb);
        sb.append(System.lineSeparator());
        print(
                "after",
                filterMap(info.getMemoryUsageAfterGc()),
                collectionInfo,
                sb);
        sb.append(System.lineSeparator());

        System.out.println(sb.toString());

        if (collectionInfo.getGcCause().equals("System.gc()")) {
            semaphore.release();
        }
    }

    private GarbageCollectionNotificationInfo obtainGcInfo(final Notification notification) {
        final CompositeData userData = (CompositeData) notification.getUserData();
        return GarbageCollectionNotificationInfo.from(userData);
    }

    private Map<String, MemoryUsage> filterMap(Map<String, MemoryUsage> gc) {
        return heapPools
                .stream()
                .collect(Collectors.toMap(
                        s -> "  " + s,
                        gc::get,
                        (u, v) -> u,
                        LinkedHashMap::new
                ));
    }

    private void print(
            String when,
            Map<String, MemoryUsage> usages,
            GarbageCollectionNotificationInfo collectionInfo,
            final StringBuilder sb) {
        printHeapUsage(usages, sb
                .append("> Heap usage on ")
                .append(collectionInfo.getGcName())
                .append(" ")
                .append(when)
                .append(" ")
                .append(collectionInfo.getGcAction())
                .append(" (due to ")
                .append(collectionInfo.getGcCause())
                .append(')')
                .append(System.lineSeparator()));
    }

    private static void printHeapUsage(
            Map<String, MemoryUsage> pools,
            StringBuilder sb) {
        int maxWidth = 0;
        for (String name : pools.keySet()) {
            maxWidth = Math.max(maxWidth, name.length());
        }
        maxWidth = (maxWidth + 1) & 0x7ffffffe;

        for (Map.Entry<String, MemoryUsage> entry : pools.entrySet()) {
            rightPad(entry.getKey(), maxWidth, sb.append("> "));
            printUsage(entry.getValue(), sb);
            sb.append(System.lineSeparator());
        }
    }

    private static void rightPad(
            final String s,
            final int width,
            final StringBuilder sb) {
        sb.append(s);
        for (int i = s.length(); i < width; i++) {
            sb.append(' ');
        }
    }

    private static void leftPad(
            final String s,
            final int width,
            final StringBuilder sb) {
        for (int i = s.length(); i < width; i++) {
            sb.append(' ');
        }
        sb.append(s);
    }

    private static void printUsage(
            final MemoryUsage usage,
            final StringBuilder sb) {
        printUnit(usage.getInit(), sb.append("initial "));
        printUnit(usage.getUsed(), sb.append(", used "));
        printUnit(usage.getCommitted(), sb.append(", committed "));
        printUnit(usage.getMax(), sb.append(", max "));
    }

    private static StringBuilder printUnit(
            long bytes,
            final StringBuilder sb) {
        final Iterator<String> it = Arrays.asList(PREFIXES).iterator();
        while (it.hasNext()) {
            String prefix = it.next();
            if (bytes >> 13 == 0 || !it.hasNext()) {
                final String s = Long.toString(bytes);
                leftPad(s, 4, sb);
                sb.append(prefix);
                return sb;
            }
            bytes = bytes >> 10;
        }
        return sb;
    }

    @Override
    public void runGc(String reason) throws InterruptedException {
        System.out.println(reason);
        // acquire 2 permits, one for minor and one for major GC
        semaphore.acquire(2);
        // Notification System Thread will release semaphore one it's done with System.gc()
        System.gc();
        // acquire here will block until GC is done
        semaphore.acquire(2);
        // immediately release lock
        semaphore.release(2);
    }
}
