package pt.uminho.haslab.saferegions.comunication;

import java.util.Arrays;

public class RequestIdentifier {

    private final byte[] requestID;

    private final byte[] regionID;

    public RequestIdentifier(byte[] requestID, byte[] regionID) {
        this.requestID = requestID;
        this.regionID = regionID;
    }

    public byte[] getRequestID() {
        return requestID;
    }

    public byte[] getRegionID() {
        return regionID;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Arrays.hashCode(this.requestID);
        hash = 29 * hash + Arrays.hashCode(this.regionID);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RequestIdentifier other = (RequestIdentifier) obj;
        if (!Arrays.equals(this.requestID, other.requestID)) {
            return false;
        }

        return Arrays.equals(this.regionID, other.regionID);
    }

}
