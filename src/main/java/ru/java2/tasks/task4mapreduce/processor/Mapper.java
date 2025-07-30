package ru.java2.tasks.task4mapreduce.processor;

import ru.java2.tasks.task4mapreduce.domain.MapReduce;

import java.util.List;

/**
 * Mapper is a functional interface for transforming an input file into
 * a list of intermediate key-value pairs for the MapReduce process.
 */
@FunctionalInterface
public interface Mapper {
    /**
     * Processes an input file and produces intermediate key-value pairs.
     *
     * @param fileName the name of the input file
     * @param content  the content of the input file
     * @return a list of key-value entries
     */
    List<MapReduce.Entry> map(String fileName, String content);
}