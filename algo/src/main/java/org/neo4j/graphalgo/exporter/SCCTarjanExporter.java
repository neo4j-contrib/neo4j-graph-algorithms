package org.neo4j.graphalgo.exporter;

import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.AbstractExporter;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class SCCTarjanExporter extends AbstractExporter<ObjectArrayList<IntSet>> {

    private IdMapping idMapping;
    private int writePropertyId;
    private AtomicInteger set = new AtomicInteger(0);

    public SCCTarjanExporter(GraphDatabaseAPI api) {
        super(api);
    }

    public SCCTarjanExporter withIdMapping(IdMapping idMapping) {
        this.idMapping = idMapping;
        return this;
    }

    public SCCTarjanExporter withWriteProperty(String writeProperty) {
        this.writePropertyId = getOrCreatePropertyId(writeProperty);
        return this;
    }

    @Override
    public void write(ObjectArrayList<IntSet> data) {
        writeInTransaction(write -> {
            for (ObjectCursor<IntSet> oCursor : data) {
                final DefinedProperty property =
                        DefinedProperty.intProperty(writePropertyId, set.incrementAndGet());
                oCursor.value.forEach((Consumer<IntCursor>) iCursor -> {
                    try {
                        write.nodeSetProperty(idMapping.toOriginalNodeId(iCursor.value),
                                property);
                    } catch (KernelException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        });
    }
}
