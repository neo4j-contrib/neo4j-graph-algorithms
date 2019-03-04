package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface SimilarityInput {
    long getId();

    static int[] indexes(long[] inputIds, List<Long> idsToFind) {
        int[] indexes = new int[idsToFind.size()];
        List<Long> missingIds = new ArrayList<>();

        int indexesFound = 0;
        for (long idToFind : idsToFind) {
            int index = Arrays.binarySearch(inputIds, idToFind);
            if (index < 0) {
                missingIds.add(idToFind);
            } else {
                indexes[indexesFound] = index;
                indexesFound++;
            }
        }

        if (!missingIds.isEmpty()) {
            throw new IllegalArgumentException(String.format("Node ids %s do not exist in node ids list", missingIds));
        }

        return indexes;
    }

    static long[] extractInputIds(SimilarityInput[] inputs) {
        return Arrays.stream(inputs).parallel().mapToLong(SimilarityInput::getId).toArray();
    }

    static int[] indexesFor(long[] inputIds, ProcedureConfiguration configuration, String key) {
        List<Long> sourceIds = configuration.get(key, Collections.emptyList());
        try {
            return indexes(inputIds, sourceIds);
        } catch(IllegalArgumentException exception) {
            String message = String.format("%s: %s", String.format("Missing node ids in '%s' list ", key), exception.getMessage());
            throw new RuntimeException(new IllegalArgumentException(message));
        }
    }

}
