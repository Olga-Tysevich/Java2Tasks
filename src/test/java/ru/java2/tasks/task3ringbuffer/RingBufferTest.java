package ru.java2.tasks.task3ringbuffer;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class RingBufferTest {

    private RingBuffer<Integer> commonBuffer;

    @BeforeEach
    void setup() {
        commonBuffer = new RingBuffer<>(RingBuffer.OverflowPolicy.THROW, RingBuffer.PollPolicy.THROW, 8);
    }

    @Test
    void testOfferAndPoll() {
        assertTrue(commonBuffer.offer(1));
        assertTrue(commonBuffer.offer(2));
        assertEquals(1, commonBuffer.poll());
        assertEquals(2, commonBuffer.poll());
        assertThrows(IllegalStateException.class, () -> commonBuffer.poll());
    }

    @Test
    void testPoll() {
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.DROP, RingBuffer.PollPolicy.RETURN_NULL, 8);
        assertNull(buffer.poll());
        assertTrue(buffer.offer(42));
        assertEquals(42, buffer.poll());
    }

    @Test
    void testSizeAndIsEmpty() {
        assertTrue(commonBuffer.isEmpty());
        commonBuffer.offer(10);
        commonBuffer.offer(20);
        assertEquals(2, commonBuffer.size());
        assertFalse(commonBuffer.isEmpty());
        commonBuffer.poll();
        assertEquals(1, commonBuffer.size());
    }

    @Test
    void testOverflowThrow() {
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.THROW, RingBuffer.PollPolicy.THROW, 2);
        assertTrue(buffer.offer(1));
        assertTrue(buffer.offer(2));
        assertThrows(IllegalStateException.class, () -> buffer.offer(3));
    }

    @Test
    void testOverflowDrop() {
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.DROP, RingBuffer.PollPolicy.RETURN_NULL, 2);
        assertTrue(buffer.offer(1));
        assertTrue(buffer.offer(2));
        assertFalse(buffer.offer(3));
    }

    @Test
    void testOverflowOverwrite() {
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.OVERWRITE, RingBuffer.PollPolicy.RETURN_NULL, 2);
        assertTrue(buffer.offer(1));
        assertTrue(buffer.offer(2));
        assertTrue(buffer.offer(3));
        assertEquals(2, buffer.poll());
        assertEquals(3, buffer.poll());
        assertNull(buffer.poll());
    }

    @Test
    void testClear() {
        assertTrue(commonBuffer.offer(1));
        assertTrue(commonBuffer.offer(2));
        commonBuffer.clear();
        assertTrue(commonBuffer.isEmpty());
        assertThrows(IllegalStateException.class, commonBuffer::poll);
    }

    @Test
    void testCloseBehavior() {
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.DROP, RingBuffer.PollPolicy.RETURN_NULL, 2);
        buffer.offer(5);
        buffer.close();
        assertThrows(IllegalStateException.class, () -> buffer.offer(6));
        assertDoesNotThrow(buffer::poll);
    }

    @Test
    void testConcurrentOfferAndPoll() throws InterruptedException {
        int threads = 10;
        int itemsPerThread = 100;
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.OVERWRITE, RingBuffer.PollPolicy.WAIT, 1000);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch producerLatch = new CountDownLatch(threads);
        CountDownLatch consumerLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                for (int j = 0; j < itemsPerThread; j++) {
                    buffer.offer(j);
                }
                producerLatch.countDown();
            });
        }
        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                for (int j = 0; j < itemsPerThread; j++) {
                    buffer.poll();
                }
                consumerLatch.countDown();
            });
        }

        producerLatch.await();
        consumerLatch.await();
        assertTrue(buffer.isEmpty());
        executor.shutdown();
    }

    @Test
    void testOverflowPoliciesInConcurrency() throws InterruptedException {
        RingBuffer<Integer> buffer = new RingBuffer<>(
                RingBuffer.OverflowPolicy.OVERWRITE,
                RingBuffer.PollPolicy.WAIT,
                3
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();

        executor.execute(() -> {
            for (int i = 0; i < 5; i++) {
                if (buffer.offer(i)) {
                    produced.incrementAndGet();
                }
            }
            latch.countDown();
        });

        executor.execute(() -> {
            for (int i = 0; i < 5; i++) {
                Integer val = buffer.poll();
                if (val != null) {
                    consumed.incrementAndGet();
                }
            }
            latch.countDown();
        });

        latch.await(2, TimeUnit.SECONDS);

        int size = buffer.size();
        assertTrue(size >= 0 && size <= 3, "Invalid buffer size: " + size);
        assertTrue(consumed.get() >= 3 && consumed.get() <= 5,
                "Consumed items: " + consumed.get());

        executor.shutdown();
    }

    @Test
    void testCloseWithConcurrency() throws InterruptedException {
        int threads = 10;
        RingBuffer<Integer> buffer = new RingBuffer<>(RingBuffer.OverflowPolicy.THROW, RingBuffer.PollPolicy.THROW, 100);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                buffer.offer(1);
                latch.countDown();
            });
        }

        latch.await();
        buffer.close();

        assertThrows(IllegalStateException.class, () -> buffer.offer(2));
        assertDoesNotThrow(buffer::poll);

        executor.shutdown();
    }
}
