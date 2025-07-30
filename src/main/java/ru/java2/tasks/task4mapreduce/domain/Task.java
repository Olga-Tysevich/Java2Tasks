package ru.java2.tasks.task4mapreduce.domain;

import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a task in the MapReduce process, such as map or reduce.
 * <p>
 * A task contains an identifier, a task type, a list of input files, and
 * information on the output files it produces. The task also maintains
 * its current status and the start time of the execution.
 */
@Getter
@Setter
@EqualsAndHashCode
public class Task {
    private final int id;
    private final TaskType type;
    private volatile TaskStatus status;
    private volatile long startTime;
    private final List<String> inputFiles;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final AtomicReference<List<String>> outputFiles = new AtomicReference<>(List.of());
    private final int reduceCount;
    private final int reduceBucket;

    public Task(int id, TaskType type, List<String> inputFiles, int reduceCount, int reduceBucket) {
        this.id = id;
        this.type = type;
        this.inputFiles = new ArrayList<>(inputFiles);
        this.reduceCount = reduceCount;
        this.reduceBucket = reduceBucket;
        this.status = TaskStatus.IDLE;
    }

    public void registerOutputFiles(List<String> files) {
        outputFiles.set(List.copyOf(files));
    }

    public List<String> getAllOutputFiles() {
        return outputFiles.get();
    }
}
