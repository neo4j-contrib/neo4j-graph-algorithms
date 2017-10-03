package org.neo4j.graphalgo.impl.msbfs;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MSBitSetBenchmark {

    @Param({
            "_1024_1024",
            "_1024",
            "_8192_8192",
            "_8192",
            "_16384_16384",
            "_16384"
    })
    public MSBFSSource source;

    private BiMultiBitSet32 bitset;
    private int[] startNodes;
    private int startNode;

    @Setup
    public void setup() {
        int nodeCount = (int) source.nodes.nodeCount();
        this.startNode = nodeCount / 2;
        if (source.sources != null) {
            startNode = source.sources.length / 2;
            startNode = Math.min(
                    startNode,
                    source.sources.length - MultiSourceBFS.OMEGA);
            startNodes = Arrays.copyOfRange(
                    source.sources,
                    startNode,
                    startNode + MultiSourceBFS.OMEGA);
        } else {
            startNodes = null;
        }
        bitset = new BiMultiBitSet32(nodeCount);
    }

    @Benchmark
    public BiMultiBitSet32 initBits() {
        if (startNodes != null) {
            bitset.setAuxBits(startNodes);
        } else {
            bitset.setAuxBits(startNode, MultiSourceBFS.OMEGA);
        }
        return bitset;
    }
}
