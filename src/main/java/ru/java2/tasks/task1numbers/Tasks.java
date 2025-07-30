package ru.java2.tasks.task1numbers;

import java.util.function.Predicate;

public abstract class Tasks {
    /**
     * A task that runs the {@link SharedIntPrinter#print(Predicate, int)} method in a separate thread.
     * <p>
     * Used to encapsulate a specific printing rule and execute it concurrently.
     * </p>
     *
     * @param printer   the shared printer instance used across threads
     * @param printRule the rule that determines if the current thread should print a number
     * @param max       the upper bound for printing
     */
    public record PrintIntTask(SharedIntPrinter printer, Predicate<Integer> printRule,
                               int max) implements Runnable {
        @Override
        public void run() {
            printer.print(printRule, max);
        }
    }
}
