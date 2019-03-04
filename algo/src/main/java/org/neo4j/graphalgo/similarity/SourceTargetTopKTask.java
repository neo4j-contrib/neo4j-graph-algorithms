package org.neo4j.graphalgo.similarity;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

class SourceTargetTopKTask<T> implements Runnable {
    private final int batchSize;
    private final int taskOffset;
    private final int multiplier;
    private final T[] ids;
    private final double similiarityCutoff;
    private final SimilarityComputer<T> computer;
    private RleDecoder decoder;
    private final Supplier<IntStream> sourceRange;
    private final Function<Integer, IntStream> targetRange;
    private final TopKConsumer<SimilarityResult>[] topKConsumers;

    SourceTargetTopKTask(int batchSize, int taskOffset, int multiplier, int length, T[] ids, double similiarityCutoff, int topK, SimilarityComputer<T> computer, RleDecoder decoder, Supplier<IntStream> sourceRange, Function<Integer, IntStream> targetRange) {
        this.batchSize = batchSize;
        this.taskOffset = taskOffset;
        this.multiplier = multiplier;
        this.ids = ids;
        this.similiarityCutoff = similiarityCutoff;
        this.computer = computer;
        this.decoder = decoder;
        this.sourceRange = sourceRange;
        this.targetRange = targetRange;
        topKConsumers = SimilarityProc.initializeTopKConsumers(length, topK);
    }

    @Override
    public void run() {
        SimilarityConsumer consumer = TopKTask.assignSimilarityPairs(topKConsumers);
        sourceRange.get().skip(taskOffset * multiplier).limit(batchSize).forEach(sourceId ->
                computeSimilarityForSourceIndex(sourceId, ids, similiarityCutoff, consumer, computer, decoder, targetRange));
    }

    private void computeSimilarityForSourceIndex(int sourceId, T[] inputs, double cutoff, SimilarityConsumer consumer, SimilarityComputer<T> computer, RleDecoder decoder, Function<Integer, IntStream> targetRange) {
        targetRange.apply(sourceId).forEach(targetId -> {
            if(sourceId != targetId) {
                SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);

                if (similarity != null) {
                    consumer.accept(sourceId, targetId, similarity);
                }
            }
        });
    }

    void mergeInto(TopKConsumer<SimilarityResult>[] target) {
        for (int i = 0; i < target.length; i++) {
            target[i].accept(topKConsumers[i]);
        }
    }
}
