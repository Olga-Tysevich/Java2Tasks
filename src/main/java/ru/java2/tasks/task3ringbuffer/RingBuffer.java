package ru.java2.tasks.task3ringbuffer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//Напишите многопоточный Ring Buffer.
/**
 * A thread-safe ring buffer (circular queue) implementation that supports customizable overflow and polling behaviors.
 *
 * <p>Elements are stored in a power-of-two sized array to allow efficient modulo operations via bit masking.
 * The buffer is implemented with two policies:</p>
 * <ul>
 *     <li>{@link OverflowPolicy} - Determines how the buffer behaves when full during {@code offer}</li>
 *     <li>{@link PollPolicy} - Determines how the buffer behaves when empty during {@code poll}</li>
 * </ul>
 *
 * @param <T> the type of elements stored in the buffer
 */
@Slf4j
@SuppressWarnings("unchecked")
public class RingBuffer<T> {
    private final Lock lock = new ReentrantLock();
    private final Condition isFull = lock.newCondition();
    private final Condition isEmpty = lock.newCondition();
    @Getter
    private final OverflowPolicy overflowPolicy;
    @Getter
    private final PollPolicy pollPolicy;
    private final Object[] buffer;
    private final int capacityMask;
    private long head = 0;
    private long tail = 0;
    private volatile boolean isClosed = false;

    /**
     * Constructs a RingBuffer with specified overflow and poll policies and capacity.
     *
     * @param overflowPolicy policy to apply when the buffer is full
     * @param pollPolicy     policy to apply when the buffer is empty
     * @param capacity       desired capacity (rounded up to the next power of two if not already)
     * @throws IllegalArgumentException if capacity is non-positive
     */
    public RingBuffer(OverflowPolicy overflowPolicy, PollPolicy pollPolicy, int capacity) {
        if (capacity <= 0) {
            log.warn("Transferred capacity: {}. Capacity must be positive", capacity);
            throw new IllegalArgumentException("Capacity must be positive");
        }

        int arrayCapacity = Integer.bitCount(capacity) == 1
                ? capacity
                : Integer.highestOneBit(capacity) << 1;

        this.buffer = new Object[arrayCapacity];
        this.capacityMask = arrayCapacity - 1;
        this.overflowPolicy = overflowPolicy;
        this.pollPolicy = pollPolicy;
    }

    /**
     * Constructs a RingBuffer with default policies (DROP on overflow, RETURN_NULL on empty).
     *
     * @param capacity buffer capacity
     */
    public RingBuffer(int capacity) {
        this(OverflowPolicy.DROP, PollPolicy.RETURN_NULL, capacity);
    }

    /**
     * Attempts to insert an element into the buffer.
     *
     * @param element element to insert
     * @return {@code true} if inserted successfully, {@code false} otherwise
     * @throws NullPointerException if the element is {@code null}
     * @throws IllegalStateException if the buffer is closed or full and THROW policy is applied
     */
    public boolean offer(T element) {
        lock.lock();
        try {
            checkClosed();
            checkElement(element);

            if (tail - head == buffer.length) {
                if (!overflowPolicy.handleFull(this)) {
                    return false;
                }
            }

            int index = (int) (tail & capacityMask);
            buffer[index] = element;
            tail++;
            isEmpty.signalAll();
            return true;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next element from the buffer.
     *
     * @return the next element, or {@code null} based on policy or if the buffer is closed and empty
     * @throws IllegalStateException if buffer is empty and THROW policy is applied
     */
    public T poll() {
        lock.lock();
        try {

            while (head == tail && !isClosed) {
                if (!pollPolicy.handleEmpty(this)) {
                    return null;
                }
            }

            if (isClosed && head == tail) {
                return null;
            }

            int index = (int) (head & capacityMask);
            T element = (T) buffer[index];
            buffer[index] = null;
            head++;
            isFull.signalAll();
            return element;
        } catch (InterruptedException e) {
            setInterrupted(e.getMessage());
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current number of elements in the buffer.
     *
     * @return the number of stored elements
     */
    public int size() {
        lock.lock();
        try {
            return (int) Math.max(0, Math.min(
                    tail - head,
                    buffer.length
            ));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns whether the buffer is currently empty.
     *
     * @return {@code true} if empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return tail == head;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Clears the buffer, removing all elements.
     */
    public void clear() {
        lock.lock();
        try {
            head = 0;
            tail = 0;
            Arrays.fill(buffer, null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the buffer. Further operations may throw exceptions or return null depending on policy.
     */
    public void close() {
        lock.lock();
        try {
            isClosed = true;
            isFull.signalAll();
            isEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void checkElement(T element) {
        if (element == null) {
            log.warn("Attempted to add null element");
            throw new NullPointerException("Cannot add null to RingBuffer");
        }
    }

    private void checkClosed() {
        if (isClosed) {
            throw new IllegalStateException("Buffer is closed");
        }
    }

    private void setInterrupted(String message) {
        log.warn("RingBuffer interrupted with exception {}. Thread: {}", message, Thread.currentThread().getName());
        Thread.currentThread().interrupt();
    }

    private void awaitNotEmpty() throws InterruptedException {
        isEmpty.await();
    }
    /**
     * Defines the policy for handling buffer overflow during insertion.
     */
    public enum OverflowPolicy {
        /**
         * Drop new elements silently when full.
         */
        DROP {
            @Override
            public boolean handleFull(RingBuffer<?> buffer) {
                return false;
            }
        },

        /**
         * Throw an exception when full.
         */
        THROW {
            @Override
            public boolean handleFull(RingBuffer<?> buffer) {
                throw new IllegalStateException("RingBuffer is full");
            }
        },

        /**
         * Overwrite the oldest element when full.
         */
        OVERWRITE {
            @Override
            public boolean handleFull(RingBuffer<?> buffer) {
                int index = (int) (buffer.head & buffer.capacityMask);
                buffer.buffer[index] = null;
                buffer.head++;
                return true;
            }
        };

        /**
         * Defines behavior when the buffer is full.
         *
         * @param buffer the buffer instance
         * @return {@code true} if the buffer can proceed to write, {@code false} otherwise
         */
        public abstract boolean handleFull(RingBuffer<?> buffer);
    }

    /**
     * Defines the policy for handling buffer underflow during polling.
     */
    public enum PollPolicy {
        /**
         * Wait until an element becomes available.
         */
        WAIT {
            @Override
            public boolean handleEmpty(RingBuffer<?> buffer) throws InterruptedException {
                buffer.awaitNotEmpty();
                return true;
            }
        },

        /**
         * Throw an exception when the buffer is empty.
         */
        THROW {
            @Override
            public boolean handleEmpty(RingBuffer<?> buffer) {
                throw new IllegalStateException("RingBuffer is empty");
            }
        },

        /**
         * Return {@code null} when the buffer is empty.
         */
        RETURN_NULL {
            @Override
            public boolean handleEmpty(RingBuffer<?> buffer) {
                return false;
            }
        };

        /**
         * Defines behavior when the buffer is empty.
         *
         * @param buffer the buffer instance
         * @return {@code true} to retry, {@code false} to exit
         * @throws InterruptedException if interrupted while waiting
         */
        public abstract boolean handleEmpty(RingBuffer<?> buffer) throws InterruptedException;
    }
}