package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

interface RelationshipImport {
    void importRelationships(int sourceGraphId, long sourceNodeId)
    throws EntityNotFoundException;

    static RelationshipImport combine(
            RelationshipImport first,
            RelationshipImport second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return (g, n) -> {
            first.importRelationships(g, n);
            second.importRelationships(g, n);
        };
    }
}
