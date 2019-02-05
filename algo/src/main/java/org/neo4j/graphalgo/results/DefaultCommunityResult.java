package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import org.HdrHistogram.Histogram;

/**
 * @author mknblch
 */
public class DefaultCommunityResult {

    public static final DefaultCommunityResult EMPTY = new DefaultCommunityResult(
            0, 0,0,0, 0, 0, -1,-1, -1, -1, -1, -1, -1, -1, -1, -1
    );

    public final long loadMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long postProcessingMillis;
    public final long nodes;
    public final long communityCount;
    public final long p1;
    public final long p5;
    public final long p10;
    public final long p25;
    public final long p50;
    public final long p75;
    public final long p90;
    public final long p95;
    public final long p99;
    public final long p100;

    public DefaultCommunityResult(long loadMillis,
                                  long computeMillis,
                                  long postProcessingMillis,
                                  long writeMillis,
                                  long nodes,
                                  long communityCount,
                                  long p100,
                                  long p99,
                                  long p95,
                                  long p90,
                                  long p75,
                                  long p50,
                                  long p25,
                                  long p10,
                                  long p5,
                                  long p1) {
        this.loadMillis = loadMillis;
        this.computeMillis = computeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.writeMillis = writeMillis;
        this.nodes = nodes;
        this.communityCount = communityCount;
        this.p100 = p100;
        this.p99 = p99;
        this.p95 = p95;
        this.p90 = p90;
        this.p75 = p75;
        this.p50 = p50;
        this.p25 = p25;
        this.p10 = p10;
        this.p5 = p5;
        this.p1 = p1;
    }

    public static class DefaultCommunityResultBuilder extends AbstractCommunityResultBuilder<DefaultCommunityResult> {

        @Override
        protected DefaultCommunityResult build(long loadMillis, long computeMillis, long writeMillis, long postProcessingMillis, long nodeCount, long communityCount, LongLongMap communitySizeMap, Histogram communityHistogram, boolean write) {
            return new DefaultCommunityResult(
                    loadMillis,
                    evalDuration,
                    postProcessingMillis,
                    writeMillis,
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
                    communityHistogram.getValueAtPercentile(1));
        }
    }
}
