package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.ProgressTimer;

/**
 * @author mknblch
 */
public abstract class AbstractResultBuilder<R> {

    protected long loadDuration = -1;
    protected long evalDuration = -1;
    protected long writeDuration = -1;

    public AbstractResultBuilder withLoadDuration(long loadDuration) {
        this.loadDuration = loadDuration;
        return this;
    }

    public AbstractResultBuilder withEvalDuration(long evalDuration) {
        this.evalDuration = evalDuration;
        return this;
    }

    public AbstractResultBuilder withWriteDuration(long writeDuration) {
        this.writeDuration = writeDuration;
        return this;
    }

    public ProgressTimer timeLoad() {
        return ProgressTimer.start(this::withLoadDuration);
    }

    public ProgressTimer timeEval() {
        return ProgressTimer.start(this::withEvalDuration);
    }

    public ProgressTimer timeWrite() {
        return ProgressTimer.start(this::withWriteDuration);
    }

    public void timeLoad(Runnable runnable) {
        try(ProgressTimer timer = timeLoad()) {
            runnable.run();
        }
    }

    public void timeEval(Runnable runnable) {
        try(ProgressTimer timer = timeEval()) {
            runnable.run();
        }
    }

    public void timeWrite(Runnable runnable) {
        try(ProgressTimer timer = timeWrite()) {
            runnable.run();
        }
    }

    public abstract R build() ;
}
