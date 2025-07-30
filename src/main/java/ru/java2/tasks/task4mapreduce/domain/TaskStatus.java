package ru.java2.tasks.task4mapreduce.domain;

/**
 * Enum representing the possible states of a {@link Task}.
 * <p>
 * A task can be in one of the following states:
 * <ul>
 *   <li>{@code IDLE} - the task has not started execution yet</li>
 *   <li>{@code IN_PROGRESS} - the task is currently being executed</li>
 *   <li>{@code COMPLETED} - the task has finished execution</li>
 * </ul>
 */
public enum TaskStatus {
    IDLE,
    IN_PROGRESS,
    COMPLETED
}
