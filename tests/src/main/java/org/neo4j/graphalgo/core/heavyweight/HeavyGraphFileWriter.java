package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.WeightMappingSerialization;
import org.neo4j.graphalgo.serialize.ByteBufferDataOutput;
import org.neo4j.graphalgo.serialize.IdMapSerialization;
import org.neo4j.graphalgo.serialize.PrivateLookup;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * @author phorn@avantgarde-labs.de
 */
public final class HeavyGraphFileWriter {

    private static final long BYTES_INT = Integer.BYTES;

    private static final MethodHandle CONTAINER = PrivateLookup.field(HeavyGraph.class, AdjacencyMatrix.class, "container");
    private static final MethodHandle ID_MAP = PrivateLookup.field(HeavyGraph.class, IdMap.class, "nodeIdMap");
    private static final MethodHandle WEIGHTS = PrivateLookup.field(HeavyGraph.class, WeightMap.class, "weights");

    public static void serialize(HeavyGraph graph, Path outFile) {

        try {
            AdjacencyMatrix container = (AdjacencyMatrix) CONTAINER.invokeExact(graph);
            IdMap nodeIdMap = (IdMap) ID_MAP.invokeExact(graph);
            WeightMap weights = (WeightMap) WEIGHTS.invokeExact(graph);

            write(
                    outFile,
                    container.outOffsets,
                    container.inOffsets,
                    container.outgoing,
                    container.incoming,
                    nodeIdMap,
                    weights
            );
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private static void write(
            Path outPath,
            int[] outOffsets,
            int[] inOffsets,
            int[][] outgoing,
            int[][] incoming,
            IdMap idMap,
            WeightMap weights) throws IOException {

        final long requiredBytes = BYTES_INT + outOffsets.length * BYTES_INT
                + BYTES_INT + inOffsets.length * BYTES_INT
                + BYTES_INT + Arrays.stream(outgoing).mapToLong(a -> BYTES_INT + a.length * BYTES_INT).sum()
                + BYTES_INT + Arrays.stream(incoming).mapToLong(a -> BYTES_INT + a.length * BYTES_INT).sum()
                + IdMapSerialization.bytes(idMap)
                + WeightMappingSerialization.bytes(weights);

        final File file = outPath.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            final FileChannel channel = raf.getChannel();
            channel.truncate(0);
            final MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, requiredBytes);
            final ByteBufferDataOutput out = new ByteBufferDataOutput(mbb);

            out.writeVInt(outOffsets.length);
            for (int outOffset : outOffsets) {
                out.writeVInt(outOffset);
            }

            out.writeVInt(inOffsets.length);
            for (int inOffset : inOffsets) {
                out.writeVInt(inOffset);
            }

            out.writeVInt(outgoing.length);
            for (int[] ints : outgoing) {
                out.writeVInt(ints.length);
                for (int i : ints) {
                    out.writeVInt(i);
                }
            }

            out.writeVInt(incoming.length);
            for (int[] ints : incoming) {
                out.writeVInt(ints.length);
                for (int i : ints) {
                    out.writeVInt(i);
                }
            }

            IdMapSerialization.write(idMap, out);
            WeightMappingSerialization.write(weights, out);

            channel.truncate(out.written());
        }
    }
}
