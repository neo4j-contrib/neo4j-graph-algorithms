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
        int topK = configuration.getInt("top",0);
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        int concurrency = configuration.getConcurrency();

        return jaccardStreamMe(ids, length, terminationFlag, concurrency, similarityCutoff, topK);
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
        int topK = configuration.getInt("top",0);

        DoubleHistogram histogram = new DoubleHistogram(5);
        AtomicLong similarityPairs = new AtomicLong();
        Stream<SimilarityResult> stream = jaccardStreamMe(ids, (int) length, terminationFlag, concurrency, similarityCutoff, topK);

        if(configuration.isWriteFlag() && similarityCutoff > 0.0) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api,
                    configuration.get("relationshipType", "SIMILAR"),
                    configuration.getWriteProperty("score"));

            Stream<SimilarityResult> similarities = stream.peek(recordInHistogram(histogram, similarityPairs));
            similarityExporter.export(similarities);

        } else {
            stream.forEach(recordInHistogram(histogram, similarityPairs));
        }

        SimilaritySummaryResult result = new SimilaritySummaryResult(
                length,
                similarityPairs.get(),
                histogram.getValueAtPercentile(50),
                histogram.getValueAtPercentile(75),
                histogram.getValueAtPercentile(90),
                histogram.getValueAtPercentile(95),
                histogram.getValueAtPercentile(99),
                histogram.getValueAtPercentile(99.9),
                histogram.getValueAtPercentile(100)
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

    private Stream<SimilarityResult> jaccardStreamMe(InputData[] ids, int length, TerminationFlag terminationFlag, int concurrency, double similarityCutoff, int topK) {
        if (concurrency == 1) {
            return jaccardStream(ids, length, similarityCutoff, topK);
        } else {
            return jaccardParallelStream(ids, length, terminationFlag, concurrency, similarityCutoff, topK);
        }
    }

    private Stream<SimilarityResult> jaccardStream(InputData[] ids, int length, double similiarityCutoff, int topK) {
        Stream<SimilarityResult> stream = IntStream.range(0, length)
                .boxed().flatMap(idx1 -> IntStream.range(idx1 + 1, length)
                        .mapToObj(idx2 -> calculateJaccard(similiarityCutoff, ids[idx1], ids[idx2])).filter(Objects::nonNull));
        return topK(stream,topK);
    }

    private Stream<SimilarityResult> jaccardParallelStream(InputData[] ids, int length, TerminationFlag terminationFlag, int concurrency, double similiarityCutoff, int topK) {

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + 1;
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int task = 0; task < taskCount; task++) {
            int taskOffset = task;
            tasks.add(() -> {
                IntStream.range(0,batchSize).forEach(offset -> {
                    int sourceId = taskOffset * multiplier + offset;
                    if (sourceId < length) {
                        IntStream.range(sourceId + 1, length).forEach(otherId -> {
                            SimilarityResult result = calculateJaccard(similiarityCutoff, ids[sourceId], ids[otherId]);
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
                put(queue, SimilarityResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<SimilarityResult> spliterator = new QueueBasedSpliterator<>(queue, SimilarityResult.TOMB, terminationFlag, timeout);
        return topK(StreamSupport.stream(spliterator, false), topK);
    }

    private Stream<SimilarityResult> topK(Stream<SimilarityResult> stream, int topK) {
        if (topK <= 0) {
            return stream;
        }
        if (topK > 10000) {
            return stream.sorted().limit(topK);
        }
        return TopKConsumer.topK(stream,topK);
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


    public static long intersection(LongHashSet targets1, LongHashSet targets2) {
        LongHashSet intersectionSet = new LongHashSet(targets1);
        intersectionSet.retainAll(targets2);
        return intersectionSet.size();
    }
    public static long intersection2(long[] targets1, long[] targets2) {
        LongHashSet intersectionSet = LongHashSet.from(targets1);
        intersectionSet.retainAll(LongHashSet.from(targets2));
        return intersectionSet.size();
    }

    // assume both are sorted
    public static long intersection3(long[] targets1, long[] targets2) {
        int len2;
        if ((len2 = targets2.length) == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (long value1 : targets1) {
            if (value1 > targets2[off2]) {
                while (++off2 != len2 && value1 > targets2[off2]);
                if (off2 == len2) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
                if (off2 == len2) return intersection;
            }
        }
        return intersection;
    }

    // idea, compute differences, when 0 then equal?
    // assume both are sorted
    public static long intersection4(long[] targets1, long[] targets2) {
        if (targets2.length == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (int off1 = 0; off1 < targets1.length; off1++) {
            if (off2 == targets2.length) return intersection;
            long value1 = targets1[off1];

            if (value1 > targets2[off2]) {
                for (;off2 < targets2.length;off2++) {
                    if (value1 <= targets2[off2]) break;
                }
                // while (++off2 != targets2.length && value1 > targets2[off2]);
                if (off2 == targets2.length) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
            }
        }
        return intersection;
    }

    // roaring bitset
    // test with JMH
}
