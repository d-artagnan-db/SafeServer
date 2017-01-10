package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.HashMap;
import java.util.Map;

public class RequestsLocks {

	private final Map<RequestIdentifier, PlayerMessageLock> locks;

	public RequestsLocks() {
		this.locks = new HashMap<RequestIdentifier, PlayerMessageLock>();
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
