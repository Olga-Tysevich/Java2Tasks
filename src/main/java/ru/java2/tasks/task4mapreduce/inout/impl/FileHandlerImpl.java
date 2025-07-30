package ru.java2.tasks.task4mapreduce.inout.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.java2.tasks.task4mapreduce.domain.MapReduce;
import ru.java2.tasks.task4mapreduce.domain.TaskType;
import ru.java2.tasks.task4mapreduce.inout.InOutHandler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import static ru.java2.tasks.task4mapreduce.utils.Constants.REDUCE_TASK_NAME;

/**
 * Implementation of {@link InOutHandler} that handles file-based I/O.
 * FileHandlerImpl is a thread-safe file I/O handler for MapReduce tasks.
 * <p>
 * Basic principles of operation:
 * <ul>
 * <li>Each MapReduce task (MAP or REDUCE) gets its own folder of the form {@code {type}-{taskId}} in {@link #outDirectory}.</li>
 * <li>For writing, a temporary file ({@code *.tmp}) with a unique name (UUID) is first created,
 * to avoid file corruption in case of process crash.</li>
 * <li>After writing, the temporary file is atomically moved to the target name to ensure data integrity.</li>
 * <li>{@link ReentrantLock} are used at the level of the (taskType-taskId) pair,
 * to prevent multiple threads from reading/writing the same file at the same time.</li>
 * <li>When the REDUCE task completes, the file is moved from the task folder to the root result folder,
 * and the temporary folder is deleted if it empties.</li>
 * <li>An internal file index {@link #fileIndex} is maintained for quick access by name.</li>
 * </ul>
 *
 * <p>Thus, the class provides:
 * <ul>
 * <li>Safe multithreaded writing and reading</li>
 * <li>Atomic replacement of files when writing</li>
 * <li>Cleaning up temporary files and empty directories</li>
 * </ul>
 */
@Slf4j
@Getter
@Setter
public class FileHandlerImpl implements InOutHandler {

    private String outDirectory = "resources/files";
    private final ConcurrentMap<String, Path> fileIndex = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, ReentrantLock> taskLocks = new ConcurrentHashMap<>();

    /**
     * Reads key-value entries from the specified file for a task.
     * <p>
     * The method uses locking to prevent concurrent access.
     * Each line of the file is split by tabs into a (key, value) pair.
     *
     * @param fileName file name
     * @param taskId   task identifier
     * @param type     task type (MAP or REDUCE)
     * @return a list of entries {@link MapReduce.Entry}
     * @throws IOException if the file does not exist or a read error occurred
     */
    @Override
    public List<MapReduce.Entry> readEntries(String fileName, int taskId, TaskType type) throws IOException {
        return withLock(type, taskId, () -> {
            Path path = requireFile(fileName);
            List<MapReduce.Entry> entries = new ArrayList<>();
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2) {
                        entries.add(new MapReduce.Entry(parts[0], parts[1]));
                    }
                }
            }
            return entries;
        });
    }

    /**
     * Reads the entire content of a file as a string for the given task.
     * <p>
     * Used when you need to get the entire contents of a file.
     *
     * @param fileName file name
     * @param taskId task ID
     * @param type task type
     * @return file contents
     * @throws IOException if file not found or read error
     */
    @Override
    public String readFile(String fileName, int taskId, TaskType type) throws IOException {
        return withLock(type, taskId, () -> {
            Path path = Paths.get(outDirectory, fileName);
            return Files.readString(path);
        });
    }

    /**
     * Writes a list of MapReduce entries to a file in a thread-safe way.
     * <p>
     * Basic steps:
     * <ol>
     * <li>Creates a folder for the task if it does not exist.</li>
     * <li>Deletes old temporary files with the same name.</li>
     * <li>Writes a new temporary file with a unique name.</li>
     * <li>Moves the file atomically to the target location.</li>
     * <li>For REDUCE files starting with {@code REDUCE_TASK_NAME},
     * the file is additionally moved to the root of the outDirectory.</li>
     * </ol>
     *
     * @param entries list of entries
     * @param fileName output file name
     * @param taskId task identifier
     * @param type task type
     * @throws IOException if a write error occurred
     */
    @Override
    public void write(List<MapReduce.Entry> entries, String fileName, int taskId, TaskType type) throws IOException {
        withLock(type, taskId, () -> {
            Path taskDir = Paths.get(outDirectory, type.name().toLowerCase() + "-" + taskId);
            Files.createDirectories(taskDir);

            Path finalPath = taskDir.resolve(fileName);
            cleanupTmpFiles(taskDir, fileName);

            Path tmpPath = writeTmpFile(entries, taskDir, fileName);
            moveAtomically(tmpPath, finalPath);

            fileIndex.put(fileName, finalPath);

            if (type == TaskType.REDUCE && fileName.startsWith(REDUCE_TASK_NAME)) {
                Path rootTarget = Paths.get(outDirectory, fileName);
                moveAtomically(finalPath, rootTarget);
                fileIndex.put(fileName, rootTarget);
                deleteDirIfEmpty(taskDir);
            }
            return null;
        });
    }

    /**
     * Deletes files associated with a task and cleans up empty directories.
     * <p>
     * The method deletes files registered in {@link #fileIndex} and deletes the folder if it is empty.
     *
     * @param inputFiles list of file names to delete
     * @param taskId task identifier
     * @param type task type
     * @throws IOException if a deletion error occurred
     */
    @Override
    public void clearFiles(List<String> inputFiles, int taskId, TaskType type) throws IOException {
        withLock(type, taskId, () -> {
            for (String fileName : inputFiles) {
                Path path = fileIndex.remove(fileName);
                if (path != null) {
                    Files.deleteIfExists(path);
                    deleteDirIfEmpty(path.getParent());
                }
            }
            return null;
        });
    }

    private Path requireFile(String fileName) throws IOException {
        Path path = fileIndex.get(fileName);
        if (path == null || !Files.exists(path)) {
            throw new FileNotFoundException("File not found: " + fileName);
        }
        return path;
    }

    private void cleanupTmpFiles(Path dir, String fileName) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, fileName + "*.tmp")) {
            for (Path tmp : stream) Files.deleteIfExists(tmp);
        }
    }

    private Path writeTmpFile(List<MapReduce.Entry> entries, Path dir, String fileName) throws IOException {
        Path tmpPath = dir.resolve(fileName + "." + UUID.randomUUID() + ".tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPath)) {
            for (MapReduce.Entry entry : entries) {
                writer.write(entry.key() + "\t" + entry.value());
                writer.newLine();
            }
        }
        return tmpPath;
    }

    private void moveAtomically(Path src, Path dest) throws IOException {
        Files.move(src, dest, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void deleteDirIfEmpty(Path dir) throws IOException {
        if (dir != null && Files.isDirectory(dir) && isDirectoryEmpty(dir)) {
            Files.delete(dir);
        }
    }

    private boolean isDirectoryEmpty(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }


    private ReentrantLock getLock(TaskType type, int taskId) {
        return taskLocks.computeIfAbsent(type.name() + "-" + taskId, k -> new ReentrantLock());
    }

    private <T> T withLock(TaskType type, int taskId, IOCallable<T> action) throws IOException {
        ReentrantLock lock = getLock(type, taskId);
        lock.lock();
        try {
            return action.call();
        } finally {
            lock.unlock();
        }
    }

    @FunctionalInterface
    private interface IOCallable<T> {
        T call() throws IOException;
    }
}

