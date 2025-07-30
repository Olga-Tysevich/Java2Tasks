package ru.java2.tasks.task4mapreduce.domain;
/**
 * This class represents a container for records used in MapReduce tasks.
 * <p>
 * A {@link} record is a simple record containing a key and a value.
 * It is used to pass data between MapReduce and Reduce tasks.
 */
public class MapReduce {

    /**
     * Represents a single key-value record in a MapReduce process.
     * <p>
     * This record stores a key and its corresponding value. It is used
     * in the Map and Reduce phases of a MapReduce job.
     *
     * @param key is the key associated with the record
     * @param value is the value associated with the record
     */
    public record Entry(String key, String value) {
    }

}