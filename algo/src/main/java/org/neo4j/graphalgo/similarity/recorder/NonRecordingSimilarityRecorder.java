package org.neo4j.graphalgo.similarity.recorder;

import org.neo4j.graphalgo.similarity.RleDecoder;
import org.neo4j.graphalgo.similarity.SimilarityComputer;
import org.neo4j.graphalgo.similarity.SimilarityResult;

public class NonRecordingSimilarityRecorder<T> implements SimilarityRecorder<T> {
    private final SimilarityComputer<T> computer;

    public NonRecordingSimilarityRecorder(SimilarityComputer computer) {
        this.computer = computer;
    }

    public long count() {
        return -1;
    }


    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {
        return computer.similarity(decoder, source, target, cutoff);
    }
}


