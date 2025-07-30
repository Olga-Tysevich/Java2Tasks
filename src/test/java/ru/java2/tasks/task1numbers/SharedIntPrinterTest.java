package ru.java2.tasks.task1numbers;


import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SharedIntPrinterTest {

    @Test
    void testEvenOddPrinting() throws InterruptedException {
        int initial = 0;
        int max = 10;

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output);

        SharedIntPrinter printer = new SharedIntPrinter(lock, condition, printStream, initial);

        Predicate<Integer> isEven = i -> i % 2 == 0;
        Predicate<Integer> isOdd = i -> i % 2 != 0;

        Thread evenThread = new Thread(new Tasks.PrintIntTask(printer, isEven, max));
        evenThread.setName("Even");
        Thread oddThread = new Thread(new Tasks.PrintIntTask(printer, isOdd, max));
        oddThread.setName("Odd");

        evenThread.start();
        oddThread.start();
        evenThread.join();
        oddThread.join();

        boolean containsAll = IntStream.range(initial, max)
                .allMatch(i -> output.toString().contains(String.valueOf(i)));

        assertTrue(containsAll, "Output should contain all numbers from " + initial + " to " + (max - 1));
    }

    @Test
    void testMultiplesPrinting() throws InterruptedException {
        int initial = 0;
        int max = 30;

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output);

        SharedIntPrinter printer = new SharedIntPrinter(lock, condition, printStream, initial);

        Predicate<Integer> isMultipleOf3 = i -> i % 3 == 0;
        Predicate<Integer> isMultipleOf5 = i -> i % 5 == 0;
        Predicate<Integer> isNotMultipleOf3And5 = i -> i % 3 != 0 && i % 5 != 0;

        Thread multiple3 = new Thread(new Tasks.PrintIntTask(printer, isMultipleOf3, max));
        Thread multiple5 = new Thread(new Tasks.PrintIntTask(printer, isMultipleOf5, max));
        Thread neither = new Thread(new Tasks.PrintIntTask(printer, isNotMultipleOf3And5, max));

        multiple3.setName("MultipleOf3");
        multiple5.setName("MultipleOf5");
        neither.setName("NotMultipleOf3And5");

        multiple3.start();
        multiple5.start();
        neither.start();

        multiple3.join();
        multiple5.join();
        neither.join();

        String outStr = output.toString();

        boolean containsAll = IntStream.range(initial, max)
                .allMatch(i -> outStr.contains(String.valueOf(i)));

        assertTrue(containsAll, "Output should contain all numbers from " + initial + " to " + (max - 1));
    }
}