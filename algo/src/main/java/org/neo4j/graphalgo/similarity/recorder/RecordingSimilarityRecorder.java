package org.neo4j.graphalgo.similarity.recorder;

import org.neo4j.graphalgo.similarity.RleDecoder;
import org.neo4j.graphalgo.similarity.SimilarityComputer;
import org.neo4j.graphalgo.similarity.SimilarityResult;

import java.util.concurrent.atomic.LongAdder;

public class RecordingSimilarityRecorder<T> implements SimilarityRecorder<T> {

    private final SimilarityComputer<T> computer;
    private final LongAdder computations = new LongAdder();

    public RecordingSimilarityRecorder(SimilarityComputer computer) {
        this.computer = computer;
    }

    public long count() {
        return computations.longValue();
    }


    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {
        computations.increment();
        return computer.similarity(decoder, source, target, cutoff);
    }
}

