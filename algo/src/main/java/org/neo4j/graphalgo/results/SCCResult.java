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
package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;

/**
 * @author mknblch
 */
public class SCCResult {

    public static SCCResult EMPTY = new SCCResult(
            0, 0, 0, 0,0, 0, -1,-1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0
    );

    public final long loadMillis;
    public final long computeMillis;
    public final long postProcessingMillis;
    public final long writeMillis;
    public final long nodes;
    public final long communityCount;
    public final long setCount;
    public final long p100;
    public final long p99;
    public final long p95;
    public final long p90;
    public final long p75;
    public final long p50;
    public final long p25;
    public final long p10;
    public final long p05;
    public final long p01;
    public final long iterations;
    public final long minSetSize;
    public final long maxSetSize;

    public SCCResult(long loadMillis, long computeMillis, long postProcessingMillis, long writeMillis, long nodes, long communityCount, long p100, long p99, long p95, long p90, long p75, long p50, long p25, long p10, long p05, long p01, long iterations, long minSetSize, long maxSetSize) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.setCount = this.communityCount = communityCount;
        this.p100 = p100;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.p25 = p25;
        this.p10 = p10;
        this.p05 = p05;
        this.p01 = p01;
        this.iterations = iterations;
        this.minSetSize = minSetSize;
        this.maxSetSize = maxSetSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends AbstractCommunityResultBuilder<SCCResult> {
        private int iterations = -1;

        @Override
        protected SCCResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram) {
            return new SCCResult(
                    loadMillis,
                    computeMillis,
                    writeMillis,
                    postProcessingMillis,
                    nodeCount,
                    communityCount,
                    communityHistogram.getValueAtPercentile(100),
                    communityHistogram.getValueAtPercentile(99),
                    communityHistogram.getValueAtPercentile(95),
                    communityHistogram.getValueAtPercentile(90),
                    communityHistogram.getValueAtPercentile(75),
                    communityHistogram.getValueAtPercentile(50),
                    communityHistogram.getValueAtPercentile(25),
                    communityHistogram.getValueAtPercentile(10),
                    communityHistogram.getValueAtPercentile(5),
                    communityHistogram.getValueAtPercentile(1),
                    iterations,
                    communityHistogram.getMinNonZeroValue(),
                    communityHistogram.getMaxValue()
            );
        }
    }

}
