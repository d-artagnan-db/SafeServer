package pt.uminho.haslab.saferegions.comunication;

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
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void signalToRead() {
        canRead.signal();
    }

    public void awaitForWrite() throws InterruptedException {
        canRead.await();
    }

    @Override
    public String toString() {
        return "PlayerMessageLock{" + "lock=" + lock + ", canRead=" + canRead
                + '}';
    }

}
