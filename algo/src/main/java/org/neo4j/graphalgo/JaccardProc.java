/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongHashSet;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.impl.util.TopKConsumer.topK;

public class JaccardProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;


    @Procedure(name = "algo.similarity.jaccard.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.jaccard.stream([{source:id, targets:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD source1, source2, count1, count2, intersection, jaccard - computes jaccard similarities")
    public Stream<SimilarityResult> jaccardStream(
            @Name(value = "data", defaultValue = "null") List<Map<String,Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        double similarityCutoff = configuration.get("similarityCutoff", -1D);
        long degreeCutoff = configuration.get("degreeCutoff", 0L);

        InputData[] ids = fillIds(data, degreeCutoff);
        int length = ids.length;
        int topN = configuration.getInt("top",0);
        int topK = configuration.getInt("topK",0);
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        return jaccardStreamMe(ids, length, terminationFlag, concurrency, similarityCutoff, topN, topK);
    }

    @Procedure(name = "algo.similarity.jaccard", mode = Mode.WRITE)
    @Description("CALL algo.similarity.jaccard([{source:id, targets:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes jaccard similarities")
    public Stream<SimilaritySummaryResult> jaccard(
            @Name(value = "data", defaultValue = "null") List<Map<String, Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);

        double similarityCutoff = configuration.get("similarityCutoff", -1D);
        long degreeCutoff = configuration.get("degreeCutoff", 0L);

        InputData[] ids = fillIds(data, degreeCutoff);
        long length = ids.length;
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        int concurrency = configuration.getConcurrency();
        int topN = configuration.getInt("top",0);
        int topK = configuration.getInt("topK",0);

        DoubleHistogram histogram = new DoubleHistogram(5);
        AtomicLong similarityPairs = new AtomicLong();
        Stream<SimilarityResult> stream = jaccardStreamMe(ids, (int) length, terminationFlag, concurrency, similarityCutoff, topN, topK);

        String writeRelationshipType = configuration.get("writeRelationshipType", "SIMILAR");
        String writeProperty = configuration.getWriteProperty("score");
        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        if(write) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty);

            Stream<SimilarityResult> similarities = stream.peek(recordInHistogram(histogram, similarityPairs));
            similarityExporter.export(similarities);

        } else {
            stream.forEach(recordInHistogram(histogram, similarityPairs));
        }

        SimilaritySummaryResult result = new SimilaritySummaryResult(
                length,
                similarityPairs.get(),
                write,
                writeRelationshipType,
                writeProperty,
                histogram.getMinValue(),
                histogram.getMaxValue(),
                histogram.getMean(),
                histogram.getStdDeviation(),
                histogram.getValueAtPercentile(25D),
                histogram.getValueAtPercentile(50D),
                histogram.getValueAtPercentile(75D),
                histogram.getValueAtPercentile(90D),
                histogram.getValueAtPercentile(95D),
                histogram.getValueAtPercentile(99D),
                histogram.getValueAtPercentile(99.9D),
                histogram.getValueAtPercentile(100D)
        );




        return Stream.of(result);
    }

    private Consumer<SimilarityResult> recordInHistogram(DoubleHistogram histogram, AtomicLong similarityPairs) {
        return result -> {
            try {
                histogram.recordValue(result.similarity);
            } catch (ArrayIndexOutOfBoundsException ignored) {

            }
            similarityPairs.getAndIncrement();
        };
    }

    private Stream<SimilarityResult> jaccardStreamMe(InputData[] ids, int length, TerminationFlag terminationFlag, int concurrency, double similarityCutoff, int topN, int topK) {
        if (concurrency == 1) {
            if (topK > 0) {
                return jaccardStreamTopK(ids, length, similarityCutoff, topN, topK);
            } else {
                return jaccardStream(ids, length, similarityCutoff, topN);
            }
        } else {
            if (topK > 0) {
                return jaccardParallelStreamTopK(ids, length, terminationFlag, concurrency, similarityCutoff, topN, topK);
            } else {
                return jaccardParallelStream(ids, length, terminationFlag, concurrency, similarityCutoff, topN);
            }
        }
    }

    private Stream<SimilarityResult> jaccardStream(InputData[] ids, int length, double similiarityCutoff, int topN) {
        Stream<SimilarityResult> stream = IntStream.range(0, length)
                .boxed().flatMap(sourceId -> IntStream.range(sourceId + 1, length)
                        .mapToObj(targetId -> calculateJaccard(similiarityCutoff, ids[sourceId], ids[targetId])).filter(Objects::nonNull));
        return topN(stream,topN);
    }

    private Stream<SimilarityResult> jaccardStreamTopK(InputData[] ids, int length, double similiarityCutoff, int topN, int topK) {
        TopKConsumer<SimilarityResult>[] topKHolder = initializeTopKConsumers(length, topK);

        for (int sourceId = 0;sourceId < length;sourceId++) {
            computeJaccardForSourceIndex(sourceId, ids, length, similiarityCutoff, (sourceIndex, targetIndex, similarityResult) -> {
                topKHolder[sourceIndex].accept(similarityResult);
                topKHolder[targetIndex].accept(similarityResult.reverse());
            });
        }
        return topN(Arrays.stream(topKHolder).flatMap(TopKConsumer::stream),topN);
    }

    interface SimilarityConsumer {
        void accept(int sourceIndex, int targetIndex, SimilarityResult result);
    }

    private TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK);
        return results;
    }

    private Stream<SimilarityResult> jaccardParallelStream(InputData[] ids, int length, TerminationFlag terminationFlag, int concurrency, double similiarityCutoff, int topN) {

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + 1;
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            int taskOffset = taskId;
            tasks.add(() -> {
                for (int offset = 0; offset < batchSize; offset++) {
                    int sourceId = taskOffset * multiplier + offset;
                    if (sourceId < length)
                        computeJaccardForSourceIndex(sourceId, ids, length, similiarityCutoff, (s, t, result) -> put(queue, result));
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
        Stream<SimilarityResult> stream = StreamSupport.stream(spliterator, false);
        return topN(stream, topN);
    }


    private Stream<SimilarityResult> jaccardParallelStreamTopK(InputData[] ids, int length, TerminationFlag terminationFlag, int concurrency, double similiarityCutoff, int topN, int topK) {
        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<TopKTask> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new TopKTask(batchSize, taskId, multiplier, length, ids, similiarityCutoff, topK));
        }

        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((TopKTask)task).mergeInto(topKConsumers);
        Stream<SimilarityResult> stream = Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
        return topN(stream, topN);
    }

    private void computeJaccardForSourceIndex(int sourceId, InputData[] ids, int length, double similiarityCutoff, SimilarityConsumer consumer) {
        for (int targetId=sourceId+1;targetId<length;targetId++) {
            SimilarityResult similarity = calculateJaccard(similiarityCutoff, ids[sourceId], ids[targetId]);
            if (similarity != null) {
                consumer.accept(sourceId, targetId, similarity);
            }
        }
    }

    private Stream<SimilarityResult> topN(Stream<SimilarityResult> stream, int topN) {
        if (topN <= 0) {
            return stream;
        }
        if (topN > 10000) {
            return stream.sorted().limit(topN);
        }
        return topK(stream,topN);
    }

    private SimilarityResult calculateJaccard(double similarityCutoff, InputData e1, InputData e2) {
        return SimilarityResult.of(e1.id, e2.id, e1.targets, e2.targets, similarityCutoff);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static class InputData implements  Comparable<InputData> {
        long id;
        long[] targets;

        public InputData(long id, long[] targets) {
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
            if ( size > degreeCutoff) {
                long[] targets = new long[size];
                int i=0;
                for (Long id : targetIds) {
                    targets[i++]=id;
                }
                Arrays.sort(targets);
                ids[idx++] = new InputData((Long) row.get("source"), targets);
            }
        }
        if (idx != ids.length) ids = Arrays.copyOf(ids, idx);
        Arrays.sort(ids);
        return ids;
    }

    private class TopKTask implements Runnable {
        private final int batchSize;
        private final int taskOffset;
        private final int multiplier;
        private final int length;
        private final InputData[] ids;
        private final double similiarityCutoff;
        private final TopKConsumer<SimilarityResult>[] topKConsumers;

        public TopKTask(int batchSize, int taskOffset, int multiplier, int length, InputData[] ids, double similiarityCutoff, int topK) {
            this.batchSize = batchSize;
            this.taskOffset = taskOffset;
            this.multiplier = multiplier;
            this.length = length;
            this.ids = ids;
            this.similiarityCutoff = similiarityCutoff;
            topKConsumers = initializeTopKConsumers(length, topK);
        }

        @Override
        public void run() {
            for (int offset = 0; offset < batchSize; offset++) {
                int sourceId = taskOffset * multiplier + offset;
                if (sourceId < length) {
                    JaccardProc.this.computeJaccardForSourceIndex(sourceId, ids, length, similiarityCutoff, (s, t, result) -> {
                        topKConsumers[s].accept(result);
                        topKConsumers[t].accept(result.reverse());
                    });
                }
            }
        }
        public void mergeInto(TopKConsumer<SimilarityResult>[] target) {
            for (int i = 0; i < target.length; i++) {
                target[i].accept(topKConsumers[i]);
            }
        }
    }


    // roaring bitset
    // test with JMH
}
