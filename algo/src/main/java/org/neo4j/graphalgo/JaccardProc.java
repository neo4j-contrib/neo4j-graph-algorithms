package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    public Stream<JaccardResult> jaccard(
            @Name(value = "data", defaultValue = "null") List<Map<String,Object>> data,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        double similiarityCutoff = ((Number) config.getOrDefault("similarityCutoff", -1D)).doubleValue();
        long degreeCutoff = ((Number) config.getOrDefault("degreeCutoff", 0L)).longValue();
        InputData[] ids = fillIds(data, degreeCutoff);
        int length = ids.length;
        return IntStream.range(0, length)
                .parallel()
                .mapToObj(idx -> idx)
                .flatMap(idx1 -> {
                    InputData e1 = ids[idx1];
                    return IntStream.range(idx1 + 1, length).mapToObj(idx2 -> {
                        InputData e2 = ids[idx2];
                        return JaccardResult.of(e1.id, e2.id, e1.targets, e2.targets, similiarityCutoff);
                    }).filter(Objects::nonNull);
                });
    }

    private static class InputData implements  Comparable<InputData> {
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
            if ( size > degreeCutoff) {
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

    public static class JaccardResult {
        public final long source2;
        public final long count1;
        public final long source1;
        public final long count2;
        public final long intersection;
        public final double jaccard;

        public JaccardResult(long source1, long source2, long count1, long count2, long intersection, double jaccard) {
            this.source1 = source1;
            this.source2 = source2;
            this.count1 = count1;
            this.count2 = count2;
            this.intersection = intersection;
            this.jaccard = jaccard;
        }

        public static JaccardResult of(long source1, long source2, LongHashSet targets1, LongHashSet targets2, double similiarityCutoff) {
            LongHashSet intersectionSet = new LongHashSet(targets1);
            intersectionSet.retainAll(targets2);
            long intersection = intersectionSet.size();
            if (similiarityCutoff >= 0d && intersection == 0) return null;
            int count1 = targets1.size();
            int count2 = targets2.size();
            long denominator = count1 + count2 - intersection;
            double jaccard = denominator == 0 ? 0 : (double)intersection / denominator;
            if (jaccard < similiarityCutoff) return null;
            return new JaccardResult(source1, source2, count1, count2, intersection, jaccard);
        }
    }
}
