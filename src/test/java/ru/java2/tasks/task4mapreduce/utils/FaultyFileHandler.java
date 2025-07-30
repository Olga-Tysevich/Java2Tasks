package ru.java2.tasks.task4mapreduce.utils;

import ru.java2.tasks.task4mapreduce.domain.MapReduce;
import ru.java2.tasks.task4mapreduce.domain.TaskType;
import ru.java2.tasks.task4mapreduce.inout.impl.FileHandlerImpl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FaultyFileHandler extends FileHandlerImpl {
    private final AtomicInteger writeFailureCounter;
    private final AtomicInteger readFailureCounter;

    public FaultyFileHandler(int writeFailureCounter,
                             int readFailureProbability) {
        this.writeFailureCounter = new AtomicInteger(writeFailureCounter);
        this.readFailureCounter = new AtomicInteger(readFailureProbability);
    }

    @Override
    public void write(List<MapReduce.Entry> entries, String fileName, int taskId, TaskType type) throws IOException {
        if (writeFailureCounter.get() > 0) {
            fail(writeFailureCounter, "Write failed by test. Attempt number: " + writeFailureCounter);
        }
        super.write(entries, fileName, taskId, type);
    }

    @Override
    public List<MapReduce.Entry> readEntries(String fileName, int taskId, TaskType type) throws IOException {
        if (readFailureCounter.get() > 0) {
            fail(readFailureCounter, "Read failed by test. Attempt number: " + readFailureCounter);
        }
        return super.readEntries(fileName, taskId, type);
    }

    private void fail(AtomicInteger counter, String message) throws IOException {
        counter.decrementAndGet();
        throw new IOException(message);
    }
}
