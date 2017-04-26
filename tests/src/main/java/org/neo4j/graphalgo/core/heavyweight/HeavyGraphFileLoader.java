package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.WeightMappingSerialization;
import org.neo4j.graphalgo.serialize.ByteBufferDataInput;
import org.neo4j.graphalgo.serialize.IdMapSerialization;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * @author phorn@avantgarde-labs.de
 */
public class HeavyGraphFileLoader {

    public static Graph load(Path inFile) throws IOException {
        final File file = inFile.toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final FileChannel channel = raf.getChannel();
            final MappedByteBuffer mbb = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size());
            final ByteBufferDataInput in = new ByteBufferDataInput(mbb);

            final int outOffsetsLength = in.readVInt();
            final int[] outOffsets = new int[outOffsetsLength];
            for (int i = 0; i < outOffsetsLength; i++) {
                outOffsets[i] = in.readVInt();
            }

            final int inOffsetsLength = in.readVInt();
            final int[] inOffsets = new int[inOffsetsLength];
            for (int i = 0; i < inOffsetsLength; i++) {
                inOffsets[i] = in.readVInt();
            }

            final int outgoingLength = in.readVInt();
            final int[][] outgoing = new int[outgoingLength][];
            for (int i = 0; i < outgoingLength; i++) {
                final int outlen = in.readVInt();
                final int[] out = new int[outlen];
                outgoing[i] = out;
                for (int j = 0; j < outlen; j++) {
                    out[j] = in.readVInt();
                }
            }

            final int incomingLength = in.readVInt();
            final int[][] incoming = new int[incomingLength][];
            for (int i = 0; i < incomingLength; i++) {
                final int inclen = in.readVInt();
                final int[] inc = new int[inclen];
                incoming[i] = inc;
                for (int j = 0; j < inclen; j++) {
                    inc[j] = in.readVInt();
                }
            }

            final int outgoingIdLength = in.readVInt();
            final long[][] outgoingIds = new long[outgoingIdLength][];
            for (int i = 0; i < outgoingIdLength; i++) {
                final int outlen = in.readVInt();
                final long[] out = new long[outlen];
                outgoingIds[i] = out;
                for (int j = 0; j < outlen; j++) {
                    out[j] = in.readVLong();
                }
            }

            final int incomingIdLength = in.readVInt();
            final long[][] incomingIds = new long[incomingIdLength][];
            for (int i = 0; i < incomingIdLength; i++) {
                final int inclen = in.readVInt();
                final long[] inc = new long[inclen];
                incomingIds[i] = inc;
                for (int j = 0; j < inclen; j++) {
                    inc[j] = in.readVLong();
                }
            }

            final AdjacencyMatrix container = new AdjacencyMatrix(
                    outOffsets,
                    inOffsets,
                    outgoing,
                    incoming,
                    outgoingIds,
                    incomingIds
            );

            final IdMap idMap = IdMapSerialization.read(in);
            final WeightMapping weightMapping = WeightMappingSerialization.read(
                    in);

            return new HeavyGraph(idMap, container, weightMapping, new NullWeightMap(1.0), new NullWeightMap(1.0));
        }
    }
}
