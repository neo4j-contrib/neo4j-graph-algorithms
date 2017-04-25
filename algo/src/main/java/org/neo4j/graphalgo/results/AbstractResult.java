package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.utils.ProgressTimer;

/**
 * TODO: remove if ProcedureCompiler can't handle inheritance
 *
 * @author mknblch
 */
public class AbstractResult {

    public final Long loadDuration;
    public final Long evalDuration;
    public final Long writeDuration;

    public AbstractResult(Long loadDuration, Long evalDuration, Long writeDuration) {
        this.loadDuration = loadDuration;
        this.evalDuration = evalDuration;
        this.writeDuration = writeDuration;
    }

    public static abstract class Builder<R> {

        protected long loadDuration = -1;
        protected long evalDuration = -1;
        protected long writeDuration = -1;

        public ProgressTimer load() {
            return ProgressTimer.start(res -> loadDuration = res);
        }

        public ProgressTimer eval() {
            return ProgressTimer.start(res -> evalDuration = res);
        }

        public ProgressTimer write() {
            return ProgressTimer.start(res -> writeDuration = res);
        }

        public abstract R build();
    }
}
