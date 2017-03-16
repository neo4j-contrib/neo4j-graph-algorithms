package org.neo4j.graphalgo.api;

import org.neo4j.kernel.api.ReadOperations;

/**
 * @author mknblch
 *         added 15.03.2017.
 */
public abstract class GraphFactory {

    protected final ReadOperations readOp;
    protected final String label;
    protected final String relation;
    protected final String property;

    // TODO collections of label/rel/prop ?
    public GraphFactory(ReadOperations readOp, String label, String relation, String property) {
        this.readOp = readOp;
        this.label = label;
        this.relation = relation;
        this.property = property;
    }

    public abstract Graph build();
}
