package pt.uminho.haslab.saferegions.middleware.messages;

import org.junit.Test;
import pt.uminho.haslab.saferegions.comunication.CIntBatchShareMessage;
import pt.uminho.haslab.saferegions.comunication.RequestIdentifier;

import java.math.BigInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CIntBatchShareMessageTest {


    @Test
    public void testEncodeDecode() {
        int sourcePlayer = 2;
        int destPlayer = 3;
        byte[] requestID = BigInteger.valueOf(112313131).toByteArray();
        byte[] regionID = BigInteger.valueOf(1231131231).toByteArray();

        RequestIdentifier reqID = new RequestIdentifier(requestID, regionID);
        int[] vals = new int[100];

        for (int i = 0; i < vals.length; i++) {
            vals[i] = 1;
        }
        CIntBatchShareMessage msg = new CIntBatchShareMessage(sourcePlayer, destPlayer, reqID, vals);
        byte[] genMessage = msg.toByteArray();
        CIntBatchShareMessage decMsg = CIntBatchShareMessage.parseFrom(genMessage);

        assertEquals(sourcePlayer, decMsg.getSourcePlayer());
        assertEquals(destPlayer, decMsg.getPlayerDest());
        assertArrayEquals(requestID, decMsg.getRequestID().getRequestID());
        assertArrayEquals(regionID, decMsg.getRequestID().getRegionID());
        assertArrayEquals(vals, decMsg.getValues());

    }

}
