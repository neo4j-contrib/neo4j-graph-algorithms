package org.neo4j.graphalgo.core;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

/**
 * @author mh
 * @since 27.06.17
 */
public class Kernel {
    private ReadOperations readOperations;

    public Kernel(Statement statement) {
        readOperations = statement.readOperations();
    }

    public int labelGetForName(String label) {
        if (label == null) return ReadOperations.NO_SUCH_LABEL;
        return readOperations.labelGetForName(label);
    }

    public int relationshipTypeGetForName(String relationship) {
        if (relationship == null) return ReadOperations.ANY_RELATIONSHIP_TYPE;
        return readOperations.relationshipTypeGetForName(relationship);
    }

    public int propertyKeyGetForName(String property) {
        if (property == null) return ReadOperations.NO_SUCH_PROPERTY_KEY;
        return readOperations.propertyKeyGetForName(property);
    }

    public long countsForNode(int labelId) {
        return readOperations.countsForNode(labelId);
    }

    public Cursor<NodeItem> nodeCursorGetAll() {
        return wrapNodes(readOperations.nodesGetAll());
    }

    private Cursor<NodeItem> wrapNodes(PrimitiveLongIterator nodes) {
        return new Cursor<NodeItem>() {
            Kernel.NodeItem item = new Kernel.NodeItem();
            @Override
            public boolean next() {
                while (true) {
                    boolean hasNext = nodes.hasNext();
                    if (!hasNext) return false;
                    long nodeId = nodes.next();
                    try {
                        item.item = readOperations.nodeCursorById(nodeId).get();
                        return true;
                    } catch (EntityNotFoundException e) {
                        // throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void close() { }

            @Override
            public NodeItem get() {
                return item;
            }
        };
    }
    private Cursor<RelationshipItem> wrapRelationships(PrimitiveLongIterator rels) {
        return new Cursor<RelationshipItem>() {
            Kernel.RelationshipItem item = new Kernel.RelationshipItem();
            @Override
            public boolean next() {
                while (true) {
                    boolean hasNext = rels.hasNext();
                    if (!hasNext) return false;
                    long relId = rels.next();
                    try {
                        item.item = readOperations.relationshipCursorById(relId).get();
                        return true;
                    } catch (EntityNotFoundException e) {
                        // throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void close() {

            }

            @Override
            public Kernel.RelationshipItem get() {
                return item;
            }
        };
    }

    public Cursor<NodeItem> nodeCursorGetForLabel(int labelId) {
        return wrapNodes(readOperations.nodesGetForLabel(labelId));
    }

    public PrimitiveLongIterator nodesGetAll() {
        return readOperations.nodesGetAll();
    }

    public PrimitiveLongIterator nodesGetForLabel(int labelId) {
        return readOperations.nodesGetForLabel(labelId);
    }

    public RelationshipIterator nodeGetRelationships(long nodeId, org.neo4j.graphdb.Direction direction, int relationTypeId) {
        try {
            if (relationTypeId == ReadOperations.ANY_RELATIONSHIP_TYPE) {
                return readOperations.nodeGetRelationships(nodeId,direction);
            } else {
                return readOperations.nodeGetRelationships(nodeId, direction, new int[]{relationTypeId});
            }
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public RelationshipIterator nodeGetRelationships(long nodeId, org.neo4j.graphdb.Direction direction) {
        try {
            return readOperations.nodeGetRelationships(nodeId,direction);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Cursor<Kernel.RelationshipItem> relationshipCursorGetAll() {
        return wrapRelationships(readOperations.relationshipsGetAll());
    }
    public PropertyItem property(RelationshipItem item, int propertyId) {
        return item.property(propertyId).get();
    }

    public Cursor<NodeItem> nodeCursor(long nodeId) {
        try {
            return new Cursor<Kernel.NodeItem>() {
                NodeItem item = new NodeItem(readOperations.nodeCursorById(nodeId).get());
                boolean first = true;

                @Override
                public boolean next() {
                    if (first) {
                        first = false;
                        return true;
                    }
                    return false;
                }

                @Override
                public void close() {
                }

                @Override
                public NodeItem get() {
                    return item;
                }
            };
        } catch (EntityNotFoundException e) {
            return EMPTY_CURSOR;
        }
    }

    public static Cursor EMPTY_CURSOR = new Cursor() {
        @Override
        public boolean next() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public Object get() {
            throw new IllegalStateException("empty cursor");
        }
    };

    static org.neo4j.graphdb.Direction direction(Direction direction) {
        if (direction == null) return null;
        switch (direction) {
            case OUTGOING: return org.neo4j.graphdb.Direction.OUTGOING;
            case INCOMING: return org.neo4j.graphdb.Direction.INCOMING;
            case BOTH: return org.neo4j.graphdb.Direction.BOTH;
        }
        throw new RuntimeException("Illegal direction "+direction);
    }

    public class NodeItem implements org.neo4j.storageengine.api.NodeItem {
        org.neo4j.storageengine.api.NodeItem item;

        public NodeItem(org.neo4j.storageengine.api.NodeItem nodeItem) {
            this.item = nodeItem;
        }

        public NodeItem() {
        }

        public Cursor<RelationshipItem> relationships(Direction direction, int relType) {
            if (relType == ReadOperations.ANY_RELATIONSHIP_TYPE)
                return wrapRelationships(nodeGetRelationships(item.id(),direction(direction)));
            else
                return wrapRelationships(nodeGetRelationships(item.id(),direction(direction),relType));
        }

        public Cursor<RelationshipItem> relationships(Direction direction) {
            return wrapRelationships(nodeGetRelationships(item.id(),direction(direction)));
        }

        public int degree(Direction direction) {
            try {
                return readOperations.nodeGetDegree(item.id(),direction(direction));
            } catch (EntityNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public int degree(Direction direction, int relType) {
            try {
                if (relType == ReadOperations.ANY_RELATIONSHIP_TYPE)
                    return readOperations.nodeGetDegree(item.id(),direction(direction));
                else
                    return readOperations.nodeGetDegree(item.id(),direction(direction),relType);
            } catch (EntityNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long id() {
            return item.id();
        }

        @Override
        public PrimitiveIntSet labels() {
            return item.labels();
        }

        @Override
        public boolean isDense() {
            return item.isDense();
        }

        @Override
        public boolean hasLabel(int i) {
            return item.hasLabel(i);
        }

        @Override
        public long nextGroupId() {
            return item.nextGroupId();
        }

        @Override
        public long nextRelationshipId() {
            return item.nextRelationshipId();
        }

        @Override
        public long nextPropertyId() {
            return item.nextPropertyId();
        }

        @Override
        public Lock lock() {
            return item.lock();
        }

        public Object propertyValue(int propertyId) {
            try {
                return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY ? null : readOperations.nodeGetProperty(item.id(), propertyId);
            } catch (EntityNotFoundException e) {
                return null;
            }
        }

        PropertyItemCursor cursor = new PropertyItemCursor();
        public Cursor<PropertyItem> property(int propertyId) {
            try {
                cursor.propertyId = propertyId;
                cursor.value = readOperations.nodeGetProperty(item.id(), propertyId);
                return cursor;
            } catch (EntityNotFoundException e) {
                // throw new RuntimeException(e);
                return EMPTY_CURSOR;
            }
        }
    }

    public class RelationshipItem implements org.neo4j.storageengine.api.RelationshipItem {
        org.neo4j.storageengine.api.RelationshipItem item;
        PropertyItemCursor cursor = new PropertyItemCursor();

        public RelationshipItem(org.neo4j.storageengine.api.RelationshipItem item) {
            this.item = item;
        }

        public RelationshipItem() {
        }

        public Object propertyValue(int propertyId) {
            try {
                return propertyId == StatementConstants.NO_SUCH_PROPERTY_KEY ? null : readOperations.relationshipGetProperty(item.id(), propertyId);
            } catch (EntityNotFoundException e) {
                return null;
            }
        }
        public Cursor<PropertyItem> property(int propertyId) {
            try {
                cursor.propertyId = propertyId;
                cursor.value = readOperations.relationshipGetProperty(item.id(), propertyId);
                return cursor;
            } catch (EntityNotFoundException e) {
                // throw new RuntimeException(e);
                return EMPTY_CURSOR;
            }
        }

        @Override
        public long id() {
            return item.id();
        }

        @Override
        public int type() {
            return item.type();
        }

        @Override
        public long startNode() {
            return item.startNode();
        }

        @Override
        public long endNode() {
            return item.endNode();
        }

        @Override
        public long otherNode(long l) {
            return item.otherNode(l);
        }

        @Override
        public long nextPropertyId() {
            return item.nextPropertyId();
        }

        @Override
        public Lock lock() {
            return item.lock();
        }
    }
    private class PropertyItemCursor implements Cursor<PropertyItem> {
        private Object value;
        private int propertyId;
        PropertyItem propertyItem = new PropertyItem() {
            @Override
            public int propertyKeyId() {
                return propertyId;
            }

            @Override
            public Object value() {
                return value;
            }
        };

        @Override
        public boolean next() {
            return value != null;
        }

        @Override
        public void close() {
        }

        @Override
        public PropertyItem get() {
            return propertyItem;
        }
    }
}
