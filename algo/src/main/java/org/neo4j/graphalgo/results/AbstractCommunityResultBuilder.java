package org.neo4j.graphalgo.results;

import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.core.utils.ProgressTimer;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

/**
 * @author mknblch
 */
public abstract class AbstractCommunityResultBuilder<T> {

    protected long loadDuration = -1;
    protected long evalDuration = -1;
    protected long writeDuration = -1;

    public AbstractCommunityResultBuilder<T> withLoadDuration(long loadDuration) {
        this.loadDuration = loadDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withEvalDuration(long evalDuration) {
        this.evalDuration = evalDuration;
        return this;
    }


    public AbstractCommunityResultBuilder<T> withWriteDuration(long writeDuration) {
        this.writeDuration = writeDuration;
        return this;
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as loadMillis
     *
     * @return
     */
    public ProgressTimer timeLoad() {
        return ProgressTimer.start(this::withLoadDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as evalMillis
     *
     * @return
     */
    public ProgressTimer timeEval() {
        return ProgressTimer.start(this::withEvalDuration);
    }

    /**
     * returns an AutoClosable which measures the time
     * until it gets closed. Saves the duration as writeMillis
     *
     * @return
     */
    public ProgressTimer timeWrite() {
        return ProgressTimer.start(this::withWriteDuration);
    }

    /**
     * evaluates loadMillis
     *
     * @param runnable
     */
    public void timeLoad(Runnable runnable) {
        try (ProgressTimer timer = timeLoad()) {
            runnable.run();
        }
    }

    /**
     * evaluates comuteMillis
     *
     * @param runnable
     */
    public void timeEval(Runnable runnable) {
        try (ProgressTimer timer = timeEval()) {
            runnable.run();
        }
    }

    /**
     * evaluates writeMillis
     *
     * @param runnable
     */
    public void timeWrite(Runnable runnable) {
        try (ProgressTimer timer = timeWrite()) {
            runnable.run();
        }
    }

    public T buildII(long nodes, IntFunction<Integer> fun) {
        return build(nodes, value -> (long) fun.apply((int) value));
    }

    public T buildLI(long nodes, LongToIntFunction fun) {
        return build(nodes, value -> (long) fun.applyAsInt(value));
    }

    /**
     * build result
     */
    public T build(long nodeCount, LongFunction<Long> fun) {

        final LongLongMap communitySizeMap = new LongLongScatterMap();
        final ProgressTimer timer = ProgressTimer.start();
        for (int i = 0; i < nodeCount; i++) {
            // map to community id
            final long cId = fun.apply(i);
            // aggregate community size
            communitySizeMap.addTo(cId, 1);
        }

        Histogram histogram = CommunityHistogram.buildFrom(communitySizeMap);

        timer.stop();

        return build(loadDuration,
                evalDuration,
                writeDuration,
                timer.getDuration(),
                nodeCount,
                communitySizeMap.size(),
                communitySizeMap,
                histogram
        );
    }

    protected abstract T build(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long postProcessingMillis,
            long nodeCount,
            long communityCount,
            LongLongMap communitySizeMap,
            Histogram communityHistogram);

}
