package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongHashSet;
import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JaccardProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.jaccard.stream", mode = Mode.READ)
    @Description("CALL algo.jaccard.stream([{source:id, targets:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD source1, source2, count1, count2, intersection, jaccard - computes jaccard similarities")
    public Stream<JaccardResult> jaccardStream(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);


        double similarityCutoff = ((Number) config.getOrDefault("similarityCutoff", -1D)).doubleValue();
        long degreeCutoff = ((Number) config.getOrDefault("degreeCutoff", 0L)).longValue();

        InputData[] ids = fillIds(data, degreeCutoff);
        int length = ids.length;
        IntStream sourceIds = IntStream.range(0, length);

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        return jaccardStreamMe(ids, length, sourceIds, terminationFlag, concurrency, similarityCutOff(similarityCutoff));
    }

    private SimilarityCutoff similarityCutOff(double similarityCutoff) {
        return new SimilarityCutoff() {
            @Override
            public boolean checkOne() {
                return similarityCutoff >= 0d;
            }

            @Override
            public boolean checkTwo(double jaccard) {
                return jaccard < similarityCutoff;
            }
        };
    }

    @Procedure(name = "algo.jaccard", mode = Mode.READ)
    @Description("CALL algo.jaccard.stream([{source:id, targets:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD source1, source2, count1, count2, intersection, jaccard - computes jaccard similarities")
    public Stream<JaccardSummaryResult> jaccard(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        double similarityCutoff = ((Number) config.getOrDefault("similarityCutoff", -1D)).doubleValue();
        long degreeCutoff = ((Number) config.getOrDefault("degreeCutoff", 0L)).longValue();

        InputData[] ids = fillIds(data, degreeCutoff);
        long length = ids.length;
        IntStream sourceIds = IntStream.range(0, (int) length);

        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        DoubleHistogram histogram = new DoubleHistogram(5);

        Stream<JaccardResult> stream = jaccardStreamMe(ids, (int) length, sourceIds, terminationFlag, concurrency, similarityCutOff(similarityCutoff));

        AtomicLong similarityPairs = new AtomicLong();
        stream.forEach(result -> {
            try {
                histogram.recordValue(result.jaccard);
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            similarityPairs.getAndIncrement();
        });

        return Stream.of(new JaccardSummaryResult(
                length,
                similarityPairs.get(),
                histogram.getValueAtPercentile(50),
                histogram.getValueAtPercentile(75),
                histogram.getValueAtPercentile(90),
                histogram.getValueAtPercentile(99)
        ));
    }

    private Stream<JaccardResult> jaccardStreamMe(InputData[] ids, int length, IntStream sourceIds, TerminationFlag terminationFlag, int concurrency, SimilarityCutoff cutoffFn) {
        if (concurrency == 1) {
            return jaccardStream(ids, length, sourceIds, terminationFlag, concurrency, cutoffFn);
        } else {
            return jaccardParallelStream(ids, length, sourceIds, terminationFlag, concurrency, cutoffFn);
        }
    }

    private Stream<JaccardResult> jaccardStream(InputData[] ids, int length, IntStream sourceIdStream, TerminationFlag terminationFlag, int concurrency, SimilarityCutoff cutoffFn) {
        return sourceIdStream
                .boxed().flatMap(idx1 -> IntStream.range(idx1 + 1, length)
                        .mapToObj(idx2 -> calculateJaccard(ids[idx1], ids[idx2], cutoffFn)).filter(Objects::nonNull));
    }

    private Stream<JaccardResult> jaccardParallelStream(InputData[] ids, int length, IntStream sourceIdStream, TerminationFlag terminationFlag, int concurrency, SimilarityCutoff cutoffFn) {

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 100);
        int taskCount = (length / batchSize) + 1;
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<JaccardResult> queue = new ArrayBlockingQueue<>(queueSize);

        int[] sourceIds = sourceIdStream.toArray();
        int multiplier = batchSize < length ? batchSize : 1;
        for (int task = 0; task < taskCount; task++) {
            int taskOffset = task;
            tasks.add(() -> {
                IntStream.range(0,batchSize).forEach(offset -> {
                    int sourceId = offset * multiplier + taskOffset;
                    if (sourceId < length) {
                        IntStream.range(sourceId + 1, length).forEach(otherId -> {
                            JaccardResult result = calculateJaccard(ids[sourceId], ids[otherId], cutoffFn);
                            if (result != null) {
                                put(queue, result);
                            }
                        });
                    }
                });
            });
        }


        new Thread(() -> {
            try {
                ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            } catch(Exception e) {
                e.printStackTrace();
            } finally {
                put(queue, JaccardResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<JaccardResult> spliterator = new QueueBasedSpliterator<>(queue, JaccardResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private JaccardResult calculateJaccard(InputData e1, InputData e2, SimilarityCutoff cutoffFun) {
        return JaccardResult.of(e1.id, e2.id, e1.targets, e2.targets, cutoffFun);
    }

    private static class InputData implements Comparable<InputData> {
        long id;
        LongHashSet targets;

        public InputData(long id, LongHashSet targets) {
            this.id = id;
            this.targets = targets;
        }

        @Override
        public int compareTo(InputData o) {
            return Long.compare(id, o.id);
        }
    }

    private InputData[] fillIds(@Name(value = "data", defaultValue = "null") List<Map<String, Object>> data, long degreeCutoff) {
        InputData[] ids = new InputData[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            List<Long> targetIds = (List<Long>) row.get("targets");
            int size = targetIds.size();
            if (size > degreeCutoff) {
                LongHashSet targets = new LongHashSet();
                for (Long id : targetIds) {
                    targets.add(id);
                }
                ids[idx++] = new InputData((Long) row.get("source"), targets);
            }
        }
        if (idx != ids.length) ids = Arrays.copyOf(ids, idx);
        Arrays.sort(ids);
        return ids;
    }

    public static class JaccardSummaryResult {

        public final Long nodes;
        public final Long similarityPairs;
        public final Double percentile50;
        public final double percentile75;
        public final double percentile90;
        public final Double percentile99;

        public JaccardSummaryResult(long nodes, long similarityPairs, double percentile50, double percentile75, double percentile90, Double percentile99) {
            this.nodes = nodes;
            this.similarityPairs = similarityPairs;
            this.percentile50 = percentile50;
            this.percentile75 = percentile75;
            this.percentile90 = percentile90;
            this.percentile99 = percentile99;
        }
    }

    interface SimilarityCutoff {
        boolean checkOne();
        boolean checkTwo(double jaccard);
    }

    public static class JaccardResult {
        public final long source1;
        public final long source2;
        public final long count1;
        public final long count2;
        public final long intersection;
        public final double jaccard;

        public static JaccardResult TOMB = new JaccardResult(-1, -1, -1, -1, -1, -1);

        public JaccardResult(long source1, long source2, long count1, long count2, long intersection, double jaccard) {
            this.source1 = source1;
            this.source2 = source2;
            this.count1 = count1;
            this.count2 = count2;
            this.intersection = intersection;
            this.jaccard = jaccard;
        }

        public static JaccardResult of(long source1, long source2, LongHashSet targets1, LongHashSet targets2, SimilarityCutoff cutoffFn) {
            long intersection = calculateIntersection(targets1, targets2);

            if (cutoffFn.checkOne() && intersection == 0) {
                return null;
            }

            int count1 = targets1.size();
            int count2 = targets2.size();
            long denominator = count1 + count2 - intersection;

            double jaccard = denominator == 0 ? 0 : (double) intersection / denominator;

            if (cutoffFn.checkTwo(jaccard)) {
                return null;
            }

            return new JaccardResult(source1, source2, count1, count2, intersection, jaccard);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JaccardResult that = (JaccardResult) o;
            return source1 == that.source1 &&
                    source2 == that.source2 &&
                    count1 == that.count1 &&
                    count2 == that.count2 &&
                    intersection == that.intersection &&
                    Double.compare(that.jaccard, jaccard) == 0;
        }

        @Override
        public int hashCode() {

            return Objects.hash(source1, source2, count1, count2, intersection, jaccard);
        }

        private static long calculateIntersection(LongHashSet targets1, LongHashSet targets2) {
            LongHashSet intersectionSet = new LongHashSet(targets1);
            intersectionSet.retainAll(targets2);
            return (long) intersectionSet.size();
        }
    }
}
