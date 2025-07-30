package ru.java2.tasks.task4mapreduce.processor;

import java.util.List;

/**
 * Reducer is a functional interface for aggregating a list of values
 * for a given key into a single reduced result.
 */
@FunctionalInterface
public interface Reducer {
    /**
     * Reduces all values associated with a key to a single output value.
     *
     * @param key    the key being reduced
     * @param values the list of values associated with the key
     * @return the reduced value as a string
     */
    String reduce(String key, List<String> values);
}
