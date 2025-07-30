package ru.java2.tasks.task4mapreduce.domain;

/**
 * Enum representing the type of task in the MapReduce framework.
 * <p>
 * There are three types of tasks:
 * <ul>
 *   <li>{@code MAP} - a task that processes data and produces intermediate key-value pairs</li>
 *   <li>{@code REDUCE} - a task that processes intermediate key-value pairs to produce final output</li>
 *   <li>{@code FINISH} - a task that is responsible for wrapping up the MapReduce job</li>
 * </ul>
 */
public enum TaskType {
    MAP,
    REDUCE,
    FINISH
}
