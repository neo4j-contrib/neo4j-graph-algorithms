/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
