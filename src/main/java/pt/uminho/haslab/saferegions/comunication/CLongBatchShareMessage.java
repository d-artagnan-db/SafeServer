package pt.uminho.haslab.saferegions.comunication;

import java.nio.ByteBuffer;

public class CLongBatchShareMessage {

    private final int sourcePlayer;
    private final int playerDest;
    private final RequestIdentifier requestID;
    private final long[] values;

    public CLongBatchShareMessage(int sourcePlayer, int playerDest, RequestIdentifier requestID, long[] values) {
        this.sourcePlayer = sourcePlayer;
        this.playerDest = playerDest;
        this.requestID = requestID;
        this.values = values;
    }

    public static CLongBatchShareMessage parseFrom(byte[] message) {
        ByteBuffer buffer = ByteBuffer.wrap(message);
        int sourcePlayer = buffer.getInt();
        int playerDest = buffer.getInt();
        int regionIdLength = buffer.getInt();
        byte[] regionID = new byte[regionIdLength];
        buffer.get(regionID, buffer.arrayOffset(), regionIdLength);
        int requestIdLength = buffer.getInt();
        byte[] requestID = new byte[requestIdLength];
        buffer.get(requestID, buffer.arrayOffset(), requestIdLength);
        int nValues = buffer.getInt();
        long[] values = new long[nValues];
        for (int i = 0; i < nValues; i++) {
            values[i] = buffer.getLong();
        }
        RequestIdentifier req = new RequestIdentifier(requestID, regionID);
        return new CLongBatchShareMessage(sourcePlayer, playerDest, req, values);
    }

    public byte[] toByteArray() {
        int msgLength = 5 * 4 + requestID.getRequestID().length + requestID.getRegionID().length + 8 * values.length;
        ByteBuffer buffer = ByteBuffer.allocate(msgLength);
        buffer.putInt(sourcePlayer);
        buffer.putInt(playerDest);
        buffer.putInt(requestID.getRegionID().length);
        buffer.put(requestID.getRegionID());
        buffer.putInt(requestID.getRequestID().length);
        buffer.put(requestID.getRequestID());
        buffer.putInt(values.length);

        for (long val : values) {
            buffer.putLong(val);
        }

        buffer.flip();
        byte[] res = buffer.array();
        buffer.clear();
        return res;

    }

    public int getSourcePlayer() {
        return sourcePlayer;
    }

    public int getPlayerDest() {
        return playerDest;
    }

    public RequestIdentifier getRequestID() {
        return requestID;
    }

    public long[] getValues() {
        return values;
    }

}
