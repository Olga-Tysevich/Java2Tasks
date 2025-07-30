package ru.java2.tasks.task1numbers;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

/**
 * Thread-safe integer printer that allows multiple threads to print integers
 * based on custom user-defined rules.
 * <p>
 * The printing is coordinated using a {@link Lock} and {@link Condition}
 * to ensure that only one thread prints at a time according to its
 * {@link Predicate} rule.
 * </p>
 *
 * <p><b>Important:</b> The correctness and termination of the program
 * depend on the user-defined {@code Predicate<Integer>} rules.
 * These predicates must be designed such that for every integer
 * up to {@code max}, at least one predicate returns {@code true}.
 * If a number is not handled by any predicate, the system will deadlock
 * due to threads endlessly waiting on the condition.
 * </p>
 *
 * <p>This class is final and designed to be used with properly constructed
 * concurrent tasks that follow a shared printing protocol.</p>
 */
@Slf4j
@AllArgsConstructor
public final class SharedIntPrinter {
    private final Lock lock;
    private final Condition condition;
    private final PrintStream out;
    private int number;

    //Пример с запуском потоков как в задании внутри junit тестов
    public void print(Predicate<Integer> printRule, int max) {
        while (true) {
            lock.lock();
            try {
                if (number > max) {
                    condition.signalAll();
                    break;
                }

                if (printRule.test(number)) {
                    out.printf("%s: %d%n%n", Thread.currentThread().getName(), number++);
                    condition.signalAll();
                }

                if (!printRule.test(number)) {
                    condition.await();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("{} was interrupted. Message: {}", Thread.currentThread().getName(), e.getMessage());
                return;
            } finally {
                lock.unlock();
            }
        }
    }
}
