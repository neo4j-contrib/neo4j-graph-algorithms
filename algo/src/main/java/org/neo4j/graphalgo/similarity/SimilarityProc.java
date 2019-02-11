package org.neo4j.graphalgo.similarity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.QueueBasedSpliterator;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.util.TopKConsumer;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.impl.util.TopKConsumer.topK;
import static org.neo4j.graphalgo.similarity.Weights.REPEAT_CUTOFF;
import static org.neo4j.helpers.collection.MapUtil.map;

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

    Long getWriteBatchSize(ProcedureConfiguration configuration) {
        return configuration.get("writeBatchSize", 10000L);
    }

    Stream<SimilaritySummaryResult> writeAndAggregateResults(Stream<SimilarityResult> stream, int length, ProcedureConfiguration configuration, boolean write, String writeRelationshipType, String writeProperty) {
        long writeBatchSize = getWriteBatchSize(configuration);
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (write) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty);
            similarityExporter.export(stream.peek(recorder), writeBatchSize);
        } else {
            stream.forEach(recorder);
        }

        return Stream.of(SimilaritySummaryResult.from(length, similarityPairs, writeRelationshipType, writeProperty, write, histogram));
    }

    Stream<SimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(SimilaritySummaryResult.from(0, new AtomicLong(0), writeRelationshipType,
                writeProperty, false, new DoubleHistogram(5)));
    }

    Double getSimilarityCutoff(ProcedureConfiguration configuration) {
        return configuration.get("similarityCutoff", -1D);
    }

    <T> Stream<SimilarityResult> similarityStream(T[] inputs, SimilarityComputer<T> computer, ProcedureConfiguration configuration, Supplier<RleDecoder> decoderFactory, double cutoff, int topK) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        int concurrency = configuration.getConcurrency();

        int length = inputs.length;
        if (concurrency == 1) {
            if (topK != 0) {
                return similarityStreamTopK(inputs, length, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityStream(inputs, length, cutoff, computer, decoderFactory);
            }
        } else {
            if (topK != 0) {
                return similarityParallelStreamTopK(inputs, length, terminationFlag, concurrency, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityParallelStream(inputs, length, terminationFlag, concurrency, cutoff, computer, decoderFactory);
            }
        }
    }

    private <T> Stream<SimilarityResult> similarityStream(T[] inputs, int length, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        RleDecoder decoder = decoderFactory.get();
        return IntStream.range(0, length)
                .boxed().flatMap(sourceId -> IntStream.range(sourceId + 1, length)
                        .mapToObj(targetId -> computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff)).filter(Objects::nonNull));
    }

    private <T> Stream<SimilarityResult> similarityStreamTopK(T[] inputs, int length, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        TopKConsumer<SimilarityResult>[] topKHolder = initializeTopKConsumers(length, topK);
        RleDecoder decoder = decoderFactory.get();

        SimilarityConsumer consumer = assignSimilarityPairs(topKHolder);
        for (int sourceId = 0; sourceId < length; sourceId++) {
            computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, consumer, computer, decoder);
        }
        return Arrays.stream(topKHolder).flatMap(TopKConsumer::stream);
    }

    private <T> Stream<SimilarityResult> similarityParallelStream(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {

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
                RleDecoder decoder = decoderFactory.get();
                for (int offset = 0; offset < batchSize; offset++) {
                    int sourceId = taskOffset * multiplier + offset;
                    if (sourceId < length)
                        computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, (s, t, result) -> put(queue, result), computer, decoder);
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

    private <T> Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<TopKTask> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new TopKTask(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer, decoderFactory.get()));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((TopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }

    private <T> void computeSimilarityForSourceIndex(int sourceId, T[] inputs, int length, double cutoff, SimilarityConsumer consumer, SimilarityComputer<T> computer, RleDecoder decoder) {
        for (int targetId = sourceId + 1; targetId < length; targetId++) {
            SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);
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

    WeightedInput[] prepareWeights(Object rawData, ProcedureConfiguration configuration, Double skipValue) throws Exception {
        if (ProcedureConstants.CYPHER_QUERY.equals(configuration.getGraphName("dense"))) {
            return prepareSparseWeights(api, (String) rawData,  skipValue, configuration);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            return preparseDenseWeights(data, getDegreeCutoff(configuration), skipValue);
        }
    }

    Double readSkipValue(ProcedureConfiguration configuration) {
        return configuration.get("skipValue", Double.NaN);
    }

    WeightedInput[] preparseDenseWeights(List<Map<String, Object>> data, long degreeCutoff, Double skipValue) {
        WeightedInput[] inputs = new WeightedInput[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {

            List<Number> weightList = extractValues(row.get("weights"));

            int size = weightList.size();
            if (size > degreeCutoff) {
                double[] weights = Weights.buildWeights(weightList);
                inputs[idx++] = skipValue == null ? WeightedInput.dense((Long) row.get("item"), weights) : WeightedInput.dense((Long) row.get("item"), weights, skipValue);
            }
        }
        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
        Arrays.sort(inputs);
        return inputs;
    }

    WeightedInput[] prepareSparseWeights(GraphDatabaseAPI api, String query, Double skipValue, ProcedureConfiguration configuration) throws Exception {
        Map<String, Object> params = configuration.getParams();
        Long degreeCutoff = getDegreeCutoff(configuration);
        int repeatCutoff = configuration.get("sparseVectorRepeatCutoff", REPEAT_CUTOFF).intValue();

        Result result = api.execute(query, params);

        Map<Long, LongDoubleMap> map = new HashMap<>();
        LongSet ids = new LongHashSet();
        result.accept((Result.ResultVisitor<Exception>) resultRow -> {
            long item = resultRow.getNumber("item").longValue();
            long id = resultRow.getNumber("category").longValue();
            ids.add(id);
            double weight = resultRow.getNumber("weight").doubleValue();
            map.compute(item, (key, agg) -> {
                if (agg == null) agg = new LongDoubleHashMap();
                agg.put(id, weight);
                return agg;
            });
            return true;
        });

        WeightedInput[] inputs = new WeightedInput[map.size()];
        int idx = 0;

        long[] idsArray = ids.toArray();
        for (Map.Entry<Long, LongDoubleMap> entry : map.entrySet()) {
            Long item = entry.getKey();
            LongDoubleMap sparseWeights = entry.getValue();

            if (sparseWeights.size() > degreeCutoff) {
                List<Number> weightsList = new ArrayList<>(ids.size());
                for (long id : idsArray) {
                    weightsList.add(sparseWeights.getOrDefault(id, skipValue));
                }
                int size = weightsList.size();
                int nonSkipSize = sparseWeights.size();
                double[] weights = Weights.buildRleWeights(weightsList, repeatCutoff);

                inputs[idx++] = WeightedInput.sparse(item, weights, size, nonSkipSize);
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

    int getTopK(ProcedureConfiguration configuration) {
        return configuration.getInt("topK", 0);
    }

    int getTopN(ProcedureConfiguration configuration) {
        return configuration.getInt("top", 0);
    }

    private Supplier<RleDecoder> createDecoderFactory(String graphType, int size) {
        if(ProcedureConstants.CYPHER_QUERY.equals(graphType)) {
            return () -> new RleDecoder(size);
        }

        return () -> null;
    }


    Supplier<RleDecoder> createDecoderFactory(ProcedureConfiguration configuration, WeightedInput input) {
        int size = input.initialSize;
        return createDecoderFactory(configuration.getGraphName("dense"), size);
    }

    interface SimilarityComputer<T> {
        SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff);
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
        private RleDecoder decoder;
        private final TopKConsumer<SimilarityResult>[] topKConsumers;

        TopKTask(int batchSize, int taskOffset, int multiplier, int length, T[] ids, double similiarityCutoff, int topK, SimilarityComputer<T> computer, RleDecoder decoder) {
            this.batchSize = batchSize;
            this.taskOffset = taskOffset;
            this.multiplier = multiplier;
            this.length = length;
            this.ids = ids;
            this.similiarityCutoff = similiarityCutoff;
            this.computer = computer;
            this.decoder = decoder;
            topKConsumers = initializeTopKConsumers(length, topK);
        }

        @Override
        public void run() {
            SimilarityConsumer consumer = assignSimilarityPairs(topKConsumers);
            for (int offset = 0; offset < batchSize; offset++) {
                int sourceId = taskOffset * multiplier + offset;
                if (sourceId < length) {

                    computeSimilarityForSourceIndex(sourceId, ids, length, similiarityCutoff, consumer, computer, decoder);
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
