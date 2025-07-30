package ru.java2.tasks.task4mapreduce.processor.impl;

import ru.java2.tasks.task4mapreduce.processor.Reducer;

import java.util.List;

/**
 * A {@link Reducer} implementation for the classic Word Count example.
 *
 * <p>Sums up all counts for a given word.</p>
 */
public class WordCountReducer implements Reducer {
    @Override
    public String reduce(String key, List<String> values) {
        int sum = values.stream().mapToInt(Integer::parseInt).sum();
        return String.valueOf(sum);
    }
}
