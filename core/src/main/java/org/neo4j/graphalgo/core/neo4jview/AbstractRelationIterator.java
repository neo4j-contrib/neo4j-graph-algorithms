package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import java.util.Iterator;

/**
 * @author mknobloch
 */
public abstract class AbstractRelationIterator<T> implements Iterator<T> {

    protected final long nodeId;
    protected final ReadOperations read;
    protected final RelationshipIterator iterator;

    protected AbstractRelationIterator(long nodeId, ReadOperations read, RelationshipIterator iterator) {
        this.nodeId = nodeId;
        this.read = read;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

}
