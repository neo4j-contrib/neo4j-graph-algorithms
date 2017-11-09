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
