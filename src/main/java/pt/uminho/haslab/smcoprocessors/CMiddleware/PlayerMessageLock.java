package pt.uminho.haslab.smcoprocessors.CMiddleware;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PlayerMessageLock {

	private final Lock lock;
	private final Condition canRead;

	public PlayerMessageLock() {
		lock = new ReentrantLock();
		canRead = lock.newCondition();

	}

	public void lock() {
		/*
		 * LOG.debug(Thread.currentThread().getName() + " lock on " +
		 * lock.toString());
		 */
		lock.lock();
	}

	public void unlock() {
		/*
		 * LOG.debug(Thread.currentThread().getName() + " unlock on " +
		 * lock.toString());
		 */
		lock.unlock();
	}

	public void signalToRead() {
		/*
		 * LOG.debug(Thread.currentThread().getName() + " signal read " +
		 * lock.toString());
		 */

		canRead.signal();
	}

	public void awaitForWrite() throws InterruptedException {
		/*
		 * LOG.debug(Thread.currentThread().getName() + " awaiting " +
		 * lock.toString());
		 */

		canRead.await();
	}

	@Override
	public String toString() {
		return "PlayerMessageLock{" + "lock=" + lock + ", canRead=" + canRead
				+ '}';
	}

}
