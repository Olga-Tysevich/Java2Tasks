package ru.java2.tasks.task4mapreduce.processor.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.java2.tasks.task4mapreduce.domain.Task;
import ru.java2.tasks.task4mapreduce.domain.TaskStatus;
import ru.java2.tasks.task4mapreduce.domain.TaskType;
import ru.java2.tasks.task4mapreduce.processor.Coordinator;
import ru.java2.tasks.task4mapreduce.validator.impl.ValidatorFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of the {@link Coordinator} interface that manages the lifecycle
 * of Map and Reduce tasks. Tasks are distributed to workers, monitored for timeouts,
 * and re-queued if not completed within the configured timeout.
 *
 * <p>Features:</p>
 * <ul>
 *     <li>Maintains idle and in-progress task queues</li>
 *     <li>Automatically reassigns timed-out tasks</li>
 *     <li>Builds reduce tasks after all map tasks complete</li>
 *     <li>Provides a FINISH task to signal that work is done</li>
 * </ul>
 */
@Slf4j
public class CoordinatorImpl implements Coordinator {

    /**
     * Initial interval (seconds) before the first timeout check is executed.
     */
    @Getter
    @Setter
    private int taskCheckInitialInterval = 10;

    /**
     * Interval (seconds) between task timeout checks.
     */
    @Getter
    @Setter
    private int taskCheckInterval = 10;

    /**
     * Maximum time (milliseconds) a task can remain in progress
     * before being considered timed out.
     */
    @Getter
    @Setter
    private int taskTimeout = 10_000;

    private final ScheduledExecutorService timeoutExecutor;
    private final Semaphore taskAvailable = new Semaphore(0);

    private final List<String> inputFiles;
    private final CopyOnWriteArrayList<Task> mapTasks;

    private final ConcurrentLinkedQueue<Task> idleTasks = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Integer, Task> inProgressTasks = new ConcurrentHashMap<>();

    private final int totalMapTasks;
    private final int totalReduceTasks;
    private final AtomicInteger completedMapTaskCounter = new AtomicInteger(0);
    private final AtomicInteger completedReduceTaskCounter = new AtomicInteger(0);
    private final AtomicBoolean reduceTasksBuilt = new AtomicBoolean(false);

    public CoordinatorImpl(List<String> inputFiles, int reduceTaskCount) {
        ValidatorFactory.listNotEmptyValidator("Input files cannot be null or empty")
                .validate(inputFiles);

        ValidatorFactory.positiveIntValidator("Reduce task count must be greater than 0")
                .validate(reduceTaskCount);


        this.totalMapTasks = inputFiles.size();
        this.totalReduceTasks = reduceTaskCount;
        this.inputFiles = inputFiles;
        this.mapTasks = new CopyOnWriteArrayList<>();

        initializeTasks();
        this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        startTimeoutChecker();
    }

    private void initializeTasks() {
        for (int i = 0; i < inputFiles.size(); i++) {
            Task task = new Task(i, TaskType.MAP, List.of(inputFiles.get(i)), totalReduceTasks, -1);
            mapTasks.add(task);
            idleTasks.add(task);
            taskAvailable.release();
        }
    }

    @Override
    public Task getTask() {
        try {
            while (true) {
                if (isDone()) {
                    return createFinishTask();
                }

                taskAvailable.acquire();
                Task task = idleTasks.poll();

                if (task == null) {
                    if (isDone()) return createFinishTask();
                    continue;
                }

                if (completedMapTaskCounter.get() == totalMapTasks) {
                    buildReduceTasks();
                }

                task.setStatus(TaskStatus.IN_PROGRESS);
                task.setStartTime(System.currentTimeMillis());
                inProgressTasks.put(task.getId(), task);
                return task;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return createFinishTask();
        }
    }

    @Override
    public void reportTask(Task task) {

        Task inProgress = inProgressTasks.remove(task.getId());
        if (inProgress == null) return;

        if (task.getType() == TaskType.MAP) {
            if (completedMapTaskCounter.incrementAndGet() == totalMapTasks) {
                log.info("All map tasks completed");
                buildReduceTasks();
            }
        } else if (task.getType() == TaskType.REDUCE) {
            if (completedReduceTaskCounter.incrementAndGet() == totalReduceTasks) {
                log.info("All reduce tasks completed");

            }
        }
        task.setStatus(TaskStatus.COMPLETED);
    }

    @Override
    public boolean isDone() {
        return completedMapTaskCounter.get() == totalMapTasks &&
                completedReduceTaskCounter.get() == totalReduceTasks;
    }

    @Override
    public void shutdown() {
        inProgressTasks.clear();
        idleTasks.clear();

        completedMapTaskCounter.set(totalMapTasks);
        completedReduceTaskCounter.set(totalReduceTasks);

        taskAvailable.release(Integer.MAX_VALUE);

        timeoutExecutor.shutdownNow();

        log.info("Shutting down coordinator. InputFiles: {}", inputFiles);
    }

    private Task createFinishTask() {
        return new Task(-1, TaskType.FINISH, List.of(""), totalReduceTasks, -1);
    }


    private void buildReduceTasks() {
        if (!reduceTasksBuilt.compareAndSet(false, true)) return;

        for (int i = 0; i < totalReduceTasks; i++) {
            List<String> bucketOut = new ArrayList<>(totalMapTasks);
            for (int j = 0; j < totalMapTasks; j++) {
                bucketOut.add(mapTasks.get(j).getAllOutputFiles().get(i));
            }
            Task reduceTask = new Task(i, TaskType.REDUCE, bucketOut, totalReduceTasks, i);
            idleTasks.add(reduceTask);
            taskAvailable.release();
        }
    }

    private void startTimeoutChecker() {
        timeoutExecutor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Task task : inProgressTasks.values()) {
                if (task.getStatus() == TaskStatus.IN_PROGRESS &&
                        now - task.getStartTime() > taskTimeout) {

                    if (inProgressTasks.remove(task.getId()) != null) {
                        task.setStatus(TaskStatus.IDLE);
                        idleTasks.add(task);
                        log.info("Task type:{}, id:{} returned to idleQueue.", task.getType(), task.getId());
                        taskAvailable.release();
                    }
                }
            }
        }, taskCheckInitialInterval, taskCheckInterval, TimeUnit.SECONDS);
    }

}