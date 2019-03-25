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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementAction;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.Collections;

final class NodesScanner extends StatementAction implements RecordScanner {

    static ImportingThreadPool.CreateScanner of(
            GraphDatabaseAPI api,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            int label,
            ImportProgress progress,
            HugeLongArrayBuilder idMapBuilder) {
        return new NodesScanner.Creator(api, scanner, label, progress, idMapBuilder);
    }

    static final class Creator implements ImportingThreadPool.CreateScanner {
        private final GraphDatabaseAPI api;
        private final AbstractStorePageCacheScanner<NodeRecord> scanner;
        private final int label;
        private final ImportProgress progress;
        private final HugeLongArrayBuilder idMapBuilder;

        Creator(
                GraphDatabaseAPI api,
                AbstractStorePageCacheScanner<NodeRecord> scanner,
                int label,
                ImportProgress progress,
                HugeLongArrayBuilder idMapBuilder) {
            this.api = api;
            this.scanner = scanner;
            this.label = label;
            this.progress = progress;
            this.idMapBuilder = idMapBuilder;
        }

        @Override
        public RecordScanner create(final int index) {
            return new NodesScanner(api, scanner, label, index, progress, idMapBuilder);
        }

        @Override
        public Collection<Runnable> flushTasks() {
            return Collections.emptyList();
        }
    }

    private final NodeStore nodeStore;
    private final AbstractStorePageCacheScanner<NodeRecord> scanner;
    private final int label;
    private final int scannerIndex;
    private final ImportProgress progress;
    private final HugeLongArrayBuilder idMapBuilder;


    private volatile long relationshipsImported;

    private NodesScanner(
            GraphDatabaseAPI api,
            AbstractStorePageCacheScanner<NodeRecord> scanner,
            int label,
            int threadIndex,
            ImportProgress progress,
            HugeLongArrayBuilder idMapBuilder) {
        super(api);
        this.nodeStore = (NodeStore) scanner.store();
        this.scanner = scanner;
        this.label = label;
        this.scannerIndex = threadIndex;
        this.progress = progress;
        this.idMapBuilder = idMapBuilder;
    }

    @Override
    public String threadName() {
        return "node-store-scan-" + scannerIndex;
    }

    @Override
    public void accept(final KernelTransaction transaction) {
        try (AbstractStorePageCacheScanner<NodeRecord>.Cursor cursor = scanner.getCursor();
             NodesBatchBuffer batches = new NodesBatchBuffer(nodeStore, label, cursor.bulkSize())) {

            final ImportProgress progress = this.progress;
            long allImported = 0L;
            while (batches.scan(cursor)) {
                int imported = importNodes(batches);
                progress.relationshipsImported(imported);
                allImported += imported;
            }
            relationshipsImported = allImported;
        }
    }

    @Override
    public long recordsImported() {
        return relationshipsImported;
    }

    private int importNodes(NodesBatchBuffer buffer) {

        int batchLength = buffer.length();
        if (batchLength == 0) {
            return 0;
        }

        HugeLongArrayBuilder.BulkAdder adder = idMapBuilder.allocate((long) (batchLength));
        if (adder == null) {
            return 0;
        }

        long[] batch = buffer.batch();
        int batchOffset = 0;
        while (adder.nextBuffer()) {
            System.arraycopy(batch, batchOffset, adder.buffer, adder.offset, adder.length);
            batchOffset += adder.length;
        }

        return batchLength;
    }
}
