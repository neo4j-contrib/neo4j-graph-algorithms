package org.neo4j.graphalgo.results;

public class MatchingAlgoResult {

    public final long loadMillis;
    public final long evalMillis;
    public final long writeMillis;
    public final String loadUuid;
    public final double offRoute;
    public final double deadHead;

    public MatchingAlgoResult(long loadMillis, long evalMillis, long writeMillis, String loadUuid, double offRoute, double deadHead) {
        this.loadMillis = loadMillis;
        this.evalMillis = evalMillis;
        this.writeMillis = writeMillis;
        this.loadUuid = loadUuid;
        this.offRoute = offRoute;
        this.deadHead = deadHead;
    }

    public static MatchingAlgoResult.Builder builder() {
        return new MatchingAlgoResult.Builder();
    }

    public static class Builder extends AbstractResultBuilder<MatchingAlgoResult>{

        protected String loadUuid = "";
        protected double offRoute = 0.0;
        protected double deadHead = 0.0;

        public MatchingAlgoResult.Builder withLoadUuid(String loadUuid) {
            this.loadUuid = loadUuid;
            return this;
        }

        public MatchingAlgoResult.Builder withOffRoute(double offRoute) {
            this.offRoute = offRoute;
            return this;
        }

        public MatchingAlgoResult.Builder withdeadHead(double deadHead){
            this.deadHead = deadHead;
            return this;
        }

        public MatchingAlgoResult build() {
            return new MatchingAlgoResult(loadDuration, evalDuration, writeDuration, loadUuid, offRoute, deadHead);
        }
    }
}
