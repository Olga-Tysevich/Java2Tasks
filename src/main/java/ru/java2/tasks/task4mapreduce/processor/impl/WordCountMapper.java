package ru.java2.tasks.task4mapreduce.processor.impl;

import ru.java2.tasks.task4mapreduce.domain.MapReduce;
import ru.java2.tasks.task4mapreduce.processor.Mapper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link Mapper} implementation for the classic Word Count example.
 *
 * <p>Splits text content into words and emits each word as a key with value "1".</p>
 */
public class WordCountMapper implements Mapper {

    @Override
    public List<MapReduce.Entry> map(String fileName, String content) {
        return Arrays.stream(content.split("\\W+"))
                .filter(word -> !word.isEmpty())
                .map(word -> new MapReduce.Entry(word.toLowerCase(), "1"))
                .collect(Collectors.toList());
    }

}
