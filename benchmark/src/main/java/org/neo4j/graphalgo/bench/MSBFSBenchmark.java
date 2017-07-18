package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.neo4jview.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MSBFSBenchmark {

    @Param({"1024", "8192", "16384"})
    public int nodeCount;

    @Param({"32", "128", "1024", "8192"})
    public int sourceCount;

    private IdMapping ids;
    private RelationshipIterator rels;
    private int[] sources;

    @Setup
    public void setup() {
        ids = new DirectIdMapping(nodeCount);
        rels = new AllNodes(nodeCount);
        sources = new int[sourceCount];
        Arrays.setAll(sources, i -> i);
    }

    @TearDown
    public void shutdown() {
        Pools.DEFAULT.shutdown();
    }

    @Benchmark
    public MultiSourceBFS measureMemory(Blackhole bh) {
        MultiSourceBFS msbfs = new MultiSourceBFS(
                ids,
                rels,
                Direction.OUTGOING,
                consume(bh),
                sources);
        msbfs.run(Pools.DEFAULT);
        return msbfs;
    }

    private static BfsConsumer consume(Blackhole bh) {
        return (i, d, s) -> bh.consume(i);
    }

    private static final class AllNodes implements RelationshipIterator {

        private final int nodeCount;

        private AllNodes(final int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        public void forEachRelationship(
                int nodeId,
                Direction direction,
                RelationshipConsumer consumer) {
            for (int i = 0; i < nodeCount; i++) {
                if (i != nodeId) {
                    consumer.accept(nodeId, i, -1L);
                }
            }
        }
    }
}
