package ru.java2.tasks.task4mapreduce.processor.impl;

import lombok.extern.slf4j.Slf4j;
import ru.java2.tasks.task4mapreduce.domain.MapReduce;
import ru.java2.tasks.task4mapreduce.domain.Task;
import ru.java2.tasks.task4mapreduce.domain.TaskType;
import ru.java2.tasks.task4mapreduce.inout.InOutHandler;
import ru.java2.tasks.task4mapreduce.processor.Coordinator;
import ru.java2.tasks.task4mapreduce.processor.Mapper;
import ru.java2.tasks.task4mapreduce.processor.Reducer;
import ru.java2.tasks.task4mapreduce.validator.Validator;
import ru.java2.tasks.task4mapreduce.validator.impl.ValidatorFactory;

import java.io.IOException;
import java.util.*;

import static ru.java2.tasks.task4mapreduce.utils.Constants.MAP_TASK_NAME_PATTERN;
import static ru.java2.tasks.task4mapreduce.utils.Constants.REDUCE_TASK_NAME_PATTERN;

/**
 * Worker is a runnable entity that continuously requests tasks from the {@link Coordinator},
 * executes them using a {@link Mapper} or {@link Reducer}, and reports the results back to the coordinator.
 *
 * <p>Responsibilities:</p>
 * <ul>
 *     <li>Fetch tasks (MAP, REDUCE, or FINISH) from the coordinator</li>
 *     <li>Process MAP tasks by splitting data into partitions for reducers</li>
 *     <li>Process REDUCE tasks by aggregating and reducing data</li>
 *     <li>Report completed tasks to the coordinator</li>
 *     <li>Stop execution upon receiving a FINISH task</li>
 * </ul>
 */
@Slf4j
public class Worker implements Runnable {
    private final Coordinator coordinator;
    private final Mapper mapper;
    private final Reducer reducer;
    private final InOutHandler handler;
    private final int reduceTaskCount;

    private Worker(Coordinator coordinator, Mapper mapper, Reducer reducer,
                   InOutHandler handler, int reduceTaskCount) {
        this.coordinator = coordinator;
        this.mapper = mapper;
        this.reducer = reducer;
        this.handler = handler;
        this.reduceTaskCount = reduceTaskCount;
    }

    /**
     * Creates a {@link Worker} instance from a given configuration.
     *
     * @param conf            the configuration object containing dependencies
     * @param reduceTaskCount the total number of reduce tasks
     * @return a new Worker instance
     */
    public static Worker fromConf(WorkerConf conf, int reduceTaskCount) {
        Validator<WorkerConf> validator = ValidatorFactory.workerConfValidator();
        validator.validateAndThrow(conf);
        return new Worker(conf.getCoordinator(), conf.getMapper(), conf.getReducer(), conf.getHandler(), reduceTaskCount);
    }

    /**
     * Continuously fetches and executes tasks until a FINISH task is received.
     * Logs the progress of task execution.
     */
    @Override
    public void run() {
        log.info("Worker started.");
        while (true) {
            Task task = coordinator.getTask();
            log.debug("Received task: ID={}, Type={}", task.getId(), task.getType());

            Validator<Task> validator = ValidatorFactory.taskValidator();
            validator.validateAndThrow(task);
            log.debug("Task validation passed for ID={}, Type={}", task.getId(), task.getType());

            if (task.getType() == TaskType.FINISH) {
                log.info("Received FINISH task. Worker is shutting down.");
                return;
            }

            try {
                if (task.getType() == TaskType.MAP) {
                    log.info("Starting MAP task: ID={}", task.getId());
                    handleMapTask(task);
                    log.info("Completed MAP task: ID={}", task.getId());
                } else if (task.getType() == TaskType.REDUCE) {
                    log.info("Starting REDUCE task: ID={}", task.getId());
                    handleReduceTask(task);
                    log.info("Completed REDUCE task: ID={}", task.getId());
                }
            } catch (Exception e) {
                log.error("Task execution failed: ID={}, Type={}, Error message={}", task.getId(), task.getType(), e.getMessage(), e);
            }
        }
    }

    /**
     * Handles a MAP task:
     * <ol>
     *     <li>Reads the input file</li>
     *     <li>Maps content into intermediate key-value entries</li>
     *     <li>Partitions entries by hash of the key for reducers</li>
     *     <li>Writes partitioned results to temporary files</li>
     *     <li>Reports the task to the coordinator</li>
     * </ol>
     *
     * @param task the MAP task to execute
     * @throws IOException if file reading or writing fails
     */
    private void handleMapTask(Task task) throws IOException {
        String content = handler.readFile(task.getInputFiles().getFirst(), task.getId(), task.getType());

        List<MapReduce.Entry> entries = mapper.map(task.getInputFiles().getFirst(), content);

        List<List<MapReduce.Entry>> partitions = new ArrayList<>(reduceTaskCount);
        for (int i = 0; i < reduceTaskCount; i++) {
            partitions.add(new ArrayList<>());
        }

        for (MapReduce.Entry entry : entries) {
            int bucket = Math.abs(entry.key().hashCode()) % reduceTaskCount;
            partitions.get(bucket).add(entry);
        }
        List<String> outputFiles = new ArrayList<>();
        for (int i = 0; i < reduceTaskCount; i++) {
            String fileName = String.format(MAP_TASK_NAME_PATTERN, task.getId(), i);
            handler.write(partitions.get(i), fileName, task.getId(), task.getType());
            outputFiles.add(fileName);
        }
        task.registerOutputFiles(outputFiles);

        coordinator.reportTask(task);
    }

    /**
     * Handles a REDUCE task:
     * <ol>
     *     <li>Reads all intermediate files from map tasks</li>
     *     <li>Groups values by key</li>
     *     <li>Applies the {@link Reducer} function to aggregate values</li>
     *     <li>Writes the final output file</li>
     *     <li>Reports the task to the coordinator and cleans up temporary files</li>
     * </ol>
     *
     * @param task the REDUCE task to execute
     * @throws IOException if file reading or writing fails
     */
    private void handleReduceTask(Task task) throws IOException {
        Map<String, List<String>> grouped = new TreeMap<>();

        for (String file : task.getInputFiles()) {
            for (MapReduce.Entry entry : handler.readEntries(file, task.getId(), task.getType())) {
                grouped.computeIfAbsent(entry.key(), k -> new ArrayList<>())
                        .add(entry.value());
            }
        }

        String outFile = String.format(REDUCE_TASK_NAME_PATTERN, task.getReduceBucket());
        List<MapReduce.Entry> results = new ArrayList<>(grouped.size());

        for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
            results.add(new MapReduce.Entry(
                    entry.getKey(),
                    reducer.reduce(entry.getKey(), entry.getValue())
            ));
        }

        handler.write(results, outFile, task.getId(), task.getType());
        task.registerOutputFiles(List.of(outFile));
        coordinator.reportTask(task);
        handler.clearFiles(task.getInputFiles(), task.getId(), TaskType.REDUCE);
    }

}
