package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.results.*;

public enum Normalization {
    NONE {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            return scores;
        }
    },
    MAX {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            double norm = scores.computeMax();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    },
    L1NORM {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            double norm = scores.computeL1Norm();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    },
    L2NORM {
        @Override
        public CentralityResult apply(CentralityResult scores) {
            double norm = scores.computeL2Norm();
            return new NormalizedCentralityResult(scores, (score) -> score / norm);
        }
    };

    public abstract CentralityResult apply(CentralityResult scores);
}
