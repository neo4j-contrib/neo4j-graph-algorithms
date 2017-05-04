package org.neo4j.graphalgo.core.utils;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;

public interface GraphExporter {

    void write(DataWriteOperations ops, int nodeId) throws KernelException;
}
