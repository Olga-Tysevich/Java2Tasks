package ru.java2.tasks.task4mapreduce.processor;

import ru.java2.tasks.task4mapreduce.domain.Task;

/**
 * The Coordinator interface defines the contract for managing and scheduling
 * tasks in a MapReduce-like processing system. Implementations are responsible
 * for distributing tasks to workers, monitoring their execution, handling
 * timeouts, and determining when all tasks are completed.
 */
public interface Coordinator {
    /**
     * Sets the interval (in seconds) at which the system checks for task timeouts.
     *
     * @param delay interval in seconds between task checks
     */
    void setTaskCheckInterval(int delay);

    /**
     * Gets the current task check interval in seconds.
     *
     * @return the task check interval
     */
    int getTaskCheckInterval();

    /**
     * Sets the timeout duration for tasks in milliseconds.
     * If a task is not reported as completed within this time,
     * it will be re-queued for execution.
     *
     * @param interval timeout in milliseconds
     */
    void setTaskTimeout(int interval);

    /**
     * Returns the currently configured task timeout in milliseconds.
     *
     * @return the task timeout
     */
    int getTaskTimeout();

    /**
     * Retrieves the next available task for execution.
     * If all tasks are completed, a special FINISH task is returned.
     *
     * @return the next task or a FINISH task if all work is done
     */
    Task getTask();

    /**
     * Reports the completion of a task.
     * If the task is a map task, the system will eventually build reduce tasks
     * when all map tasks are completed.
     *
     * @param task the completed task
     */
    void reportTask(Task task);

    /**
     * Checks if all map and reduce tasks have been completed.
     *
     * @return true if no tasks are left, false otherwise
     */
    boolean isDone();

    /**
     * Gracefully shuts down the coordinator, clears all queues,
     * and stops any background timeout checking.
     */
    void shutdown();
}
