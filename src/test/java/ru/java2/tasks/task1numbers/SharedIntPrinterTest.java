package ru.java2.tasks.task1numbers;


import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SharedIntPrinterTest {

    @RepeatedTest(value = 100, name = "{displayName} - повтор {currentRepetition}/{totalRepetitions}")
    void testEvenOddPrinting(RepetitionInfo repetitionInfo) throws InterruptedException {
        System.out.println("Запуск №" + repetitionInfo.getCurrentRepetition());
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

        String outputString = output.toString();
        Map<String, Predicate<Integer>> predicates = Map.of("Even", isEven, "Odd", isOdd);

        Map<String, List<Integer>> results = parseOutput(outputString, predicates);

        List<Integer> evenNumbersFromOutput = results.get("Even");
        List<Integer> oddNumbersFromOutput = results.get("Odd");

        assertEquals(6, evenNumbersFromOutput.size(), "Expected even number list size: 6, actual: " + evenNumbersFromOutput.size());
        assertEquals(5, oddNumbersFromOutput.size(), "Expected odd number list size: 5, actual: " + oddNumbersFromOutput.size());

        assertTrue(evenNumbersFromOutput.stream().allMatch(isEven),
                "All numbers printed by Even thread should be even");

        assertTrue(oddNumbersFromOutput.stream().allMatch(isOdd),
                "All numbers printed by Odd thread should be odd");
    }


    @RepeatedTest(value = 100, name = "{displayName} - повтор {currentRepetition}/{totalRepetitions}")
    void testMultiplesPrinting(RepetitionInfo repetitionInfo) throws InterruptedException {
        System.out.println("Запуск №" + repetitionInfo.getCurrentRepetition());
        int initial = 0;
        int max = 30;

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output);

        SharedIntPrinter printer = new SharedIntPrinter(lock, condition, printStream, initial);

        Predicate<Integer> isMultipleOf3NotMultipleOf5 = i -> i % 3 == 0;
        Predicate<Integer> isMultipleOf5NotMultipleOf3 = i -> i % 5 == 0;
        Predicate<Integer> isNotMultipleOf3And5 = i -> i % 3 != 0 && i % 5 != 0;

        Thread multiple3 = new Thread(new Tasks.PrintIntTask(printer, isMultipleOf3NotMultipleOf5, max));
        Thread multiple5 = new Thread(new Tasks.PrintIntTask(printer, isMultipleOf5NotMultipleOf3, max));
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

        String outputString = output.toString();
        Map<String, Predicate<Integer>> predicates = Map.of("MultipleOf3", isMultipleOf3NotMultipleOf5, "MultipleOf5", isMultipleOf5NotMultipleOf3,
                "NotMultipleOf3And5", isNotMultipleOf3And5);

        Map<String, List<Integer>> results = parseOutput(outputString, predicates);

        results.forEach((threadName, numbers) -> {
            Predicate<Integer> predicate = predicates.get(threadName);
            assertTrue(numbers.stream().allMatch(predicate),
                    () -> "All numbers from " + threadName + " should match its predicate");
        });
    }

    private Map<String, List<Integer>> parseOutput(String outputString, Map<String, Predicate<Integer>> predicates) {
        String[] outputLines = outputString.split("\n");

        Map<String, List<Integer>> result = new HashMap<>();

        for (String line : outputLines) {
            if (line.trim().isEmpty()) continue;
            String[] parts = line.split(": ");
            String name = parts[0];
            int number = Integer.parseInt(parts[1].trim());

            Predicate<Integer> predicate = predicates.get(name);
            if (predicate.test(number)) {
                result.computeIfAbsent(name, k -> new ArrayList<>()).add(number);
            }
        }

        return result;
    }

}