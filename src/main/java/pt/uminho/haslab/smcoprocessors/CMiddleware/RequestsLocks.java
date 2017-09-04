package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RequestsLocks {

    private final Map<RequestIdentifier, PlayerMessageLock> locks;

    public RequestsLocks() {
        this.locks = new ConcurrentHashMap<RequestIdentifier, PlayerMessageLock>();
    }

    public void signalToRead(RequestIdentifier requestID) {
        locks.get(requestID).signalToRead();
    }

    public void lockOnRequest(RequestIdentifier requestID) {
        if (locks.containsKey(requestID)) {
            locks.get(requestID).lock();
        } else {
            PlayerMessageLock pml = new PlayerMessageLock();
            locks.put(requestID, pml);
            pml.lock();
        }

    }

    public void unlockOnRequest(RequestIdentifier requestID) {
        locks.get(requestID).unlock();
    }

    public void awaitForWrite(RequestIdentifier requestID)
            throws InterruptedException {

        //System.out.println("Locks "+locks);
        //System.out.println("Locks req "+ requestID + " - " +locks.get(requestID));
        locks.get(requestID).awaitForWrite();
    }

    public void removeLock(RequestIdentifier requestID) {
        locks.remove(requestID);
    }

    public int countLocks() {
        return this.locks.size();
    }

    public boolean lockExist(RequestIdentifier requestID) {
        return this.locks.containsKey(requestID);
    }

}
