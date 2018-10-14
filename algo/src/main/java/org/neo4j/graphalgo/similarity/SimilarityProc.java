package org.neo4j.graphalgo.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.util.TopKConsumer;
import org.neo4j.graphalgo.impl.yens.SimilarityExporter;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.impl.util.TopKConsumer.topK;

public class SimilarityProc {
    @Context
    public GraphDatabaseAPI api;
    @Context
    public Log log;
    @Context
    public KernelTransaction transaction;

    private static TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK, comparator);
        return results;
    }

    static Stream<SimilarityResult> topN(Stream<SimilarityResult> stream, int topN) {
        if (topN == 0) {
            return stream;
        }
        Comparator<SimilarityResult> comparator = topN > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topN = Math.abs(topN);

        if (topN > 10000) {
            return stream.sorted(comparator).limit(topN);
        }
        return topK(stream, topN, comparator);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    Long getDegreeCutoff(ProcedureConfiguration configuration) {
        return configuration.get("degreeCutoff", 0L);
    }

    Stream<SimilaritySummaryResult> writeAndAggregateResults(ProcedureConfiguration configuration, Stream<SimilarityResult> stream, int length, boolean write, String defaultWriteProperty) {
        String writeRelationshipType = configuration.get("writeRelationshipType", defaultWriteProperty);
        String writeProperty = configuration.getWriteProperty("score");

        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (write) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty);
            similarityExporter.export(stream.peek(recorder));
        } else {
            stream.forEach(recorder);
        }

        return Stream.of(SimilaritySummaryResult.from(length, similarityPairs, writeRelationshipType, writeProperty, write, histogram));
    }

    Double getSimilarityCutoff(ProcedureConfiguration configuration) {
        return configuration.get("similarityCutoff", 0.1D);
    }

    <T> Stream<SimilarityResult> similarityStream(T[] inputs, SimilarityComputer<T> computer, ProcedureConfiguration configuration, double cutoff, int topK) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        int concurrency = configuration.getConcurrency();

        int length = inputs.length;
        if (concurrency == 1) {
            if (topK != 0) {
                return similarityStreamTopK(inputs, length, cutoff, topK, computer);
            } else {
                return similarityStream(inputs, length, cutoff, computer);
            }
        } else {
            if (topK != 0) {
                return similarityParallelStreamTopK(inputs, length, terminationFlag, concurrency, cutoff, topK, computer);
            } else {
                return similarityParallelStream(inputs, length, terminationFlag, concurrency, cutoff, computer);
            }
        }
    }

    private <T> Stream<SimilarityResult> similarityStream(T[] inputs, int length, double similiarityCutoff, SimilarityComputer<T> computer) {
        return IntStream.range(0, length)
                .boxed().flatMap(sourceId -> IntStream.range(sourceId + 1, length)
                        .mapToObj(targetId -> computer.similarity(inputs[sourceId], inputs[targetId], similiarityCutoff)).filter(Objects::nonNull));
    }

    private <T> Stream<SimilarityResult> similarityStreamTopK(T[] inputs, int length, double cutoff, int topK, SimilarityComputer<T> computer) {
        TopKConsumer<SimilarityResult>[] topKHolder = initializeTopKConsumers(length, topK);

        SimilarityConsumer consumer = assignSimilarityPairs(topKHolder);
        for (int sourceId = 0; sourceId < length; sourceId++) {
            computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, consumer, computer);
        }
        return Arrays.stream(topKHolder).flatMap(TopKConsumer::stream);
    }

    private <T> Stream<SimilarityResult> similarityParallelStream(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, SimilarityComputer<T> computer) {

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            int taskOffset = taskId;
            tasks.add(() -> {
                for (int offset = 0; offset < batchSize; offset++) {
                    int sourceId = taskOffset * multiplier + offset;
                    if (sourceId < length)
                        computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, (s, t, result) -> put(queue, result), computer);
                }
            });
        }

        new Thread(() -> {
            try {
                ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            } finally {
                put(queue, SimilarityResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<SimilarityResult> spliterator = new QueueBasedSpliterator<>(queue, SimilarityResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    private <T> Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityComputer<T> computer) {
        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<TopKTask> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new TopKTask(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((TopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }

    private <T> void computeSimilarityForSourceIndex(int sourceId, T[] inputs, int length, double cutoff, SimilarityConsumer consumer, SimilarityComputer<T> computer) {
        for (int targetId = sourceId + 1; targetId < length; targetId++) {
            SimilarityResult similarity = computer.similarity(inputs[sourceId], inputs[targetId], cutoff);
            if (similarity != null) {
                consumer.accept(sourceId, targetId, similarity);
            }
        }
    }

    CategoricalInput[] prepareCategories(List<Map<String, Object>> data, long degreeCutoff) {
        CategoricalInput[] ids = new CategoricalInput[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            List<Number> targetIds = extractValues(row.get("categories"));
            int size = targetIds.size();
            if (size > degreeCutoff) {
                long[] targets = new long[size];
                int i = 0;
                for (Number id : targetIds) {
                    targets[i++] = id.longValue();
                }
                Arrays.sort(targets);
                ids[idx++] = new CategoricalInput((Long) row.get("item"), targets);
            }
        }
        if (idx != ids.length) ids = Arrays.copyOf(ids, idx);
        Arrays.sort(ids);
        return ids;
    }

    WeightedInput[] prepareWeights(List<Map<String, Object>> data, long degreeCutoff) {
        WeightedInput[] inputs = new WeightedInput[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {

            List<Number> weightList = extractValues(row.get("weights"));

            int size = weightList.size();
            if (size > degreeCutoff) {
                double[] weights = new double[size];
                int i = 0;
                for (Number value : weightList) {
                    weights[i++] = value.doubleValue();
                }
                inputs[idx++] = new WeightedInput((Long) row.get("item"), weights);
            }
        }
        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
        Arrays.sort(inputs);
        return inputs;
    }

    private List<Number> extractValues(Object rawValues) {
        if (rawValues == null) {
            return Collections.emptyList();
        }

        List<Number> valueList = new ArrayList<>();
        if (rawValues instanceof long[]) {
            long[] values = (long[]) rawValues;
            for (long value : values) {
                valueList.add(value);
            }
        } else if (rawValues instanceof double[]) {
            double[] values = (double[]) rawValues;
            for (double value : values) {
                valueList.add(value);
            }
        } else {
            valueList = (List<Number>) rawValues;
        }
        return valueList;
    }

    protected int getTopK(ProcedureConfiguration configuration) {
        return configuration.getInt("topK", 0);
    }

    protected int getTopN(ProcedureConfiguration configuration) {
        return configuration.getInt("top", 0);
    }

    interface SimilarityComputer<T> {
        SimilarityResult similarity(T source, T target, double cutoff);
    }

    public static SimilarityConsumer assignSimilarityPairs(TopKConsumer<SimilarityResult>[] topKConsumers) {
        return (s, t, result) -> {
            topKConsumers[result.reversed ? t : s].accept(result);

            if (result.bidirectional) {
                SimilarityResult reverse = result.reverse();
                topKConsumers[reverse.reversed ? t : s].accept(reverse);
            }
        };
    }

    private class TopKTask<T> implements Runnable {
        private final int batchSize;
        private final int taskOffset;
        private final int multiplier;
        private final int length;
        private final T[] ids;
        private final double similiarityCutoff;
        private final SimilarityComputer<T> computer;
        private final TopKConsumer<SimilarityResult>[] topKConsumers;

        TopKTask(int batchSize, int taskOffset, int multiplier, int length, T[] ids, double similiarityCutoff, int topK, SimilarityComputer<T> computer) {
            this.batchSize = batchSize;
            this.taskOffset = taskOffset;
            this.multiplier = multiplier;
            this.length = length;
            this.ids = ids;
            this.similiarityCutoff = similiarityCutoff;
            this.computer = computer;
            topKConsumers = initializeTopKConsumers(length, topK);
        }

        @Override
        public void run() {
            SimilarityConsumer consumer = assignSimilarityPairs(topKConsumers);
            for (int offset = 0; offset < batchSize; offset++) {
                int sourceId = taskOffset * multiplier + offset;
                if (sourceId < length) {

                    computeSimilarityForSourceIndex(sourceId, ids, length, similiarityCutoff, consumer, computer);
                }
            }
        }


        void mergeInto(TopKConsumer<SimilarityResult>[] target) {
            for (int i = 0; i < target.length; i++) {
                target[i].accept(topKConsumers[i]);
            }
        }
    }
}
