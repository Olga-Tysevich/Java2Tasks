package ru.java2.tasks.task4mapreduce.inout;

import ru.java2.tasks.task4mapreduce.domain.MapReduce;
import ru.java2.tasks.task4mapreduce.domain.TaskType;

import java.io.IOException;
import java.util.List;

/**
 * Handles input/output operations for MapReduce tasks.
 */
public interface InOutHandler {
    /**
     * Sets the output directory for files.
     *
     * @param outputDirectory directory path where output files will be stored
     */
    void setOutDirectory(String outputDirectory);

    /**
     * Reads key-value entries from a file for the given task.
     *
     * @param fileName the name of the file to read
     * @param taskId   the task ID
     * @param type     the task type
     * @return a list of MapReduce entries read from the file
     * @throws IOException if an I/O error occurs
     */
    List<MapReduce.Entry> readEntries(String fileName, int taskId, TaskType type) throws IOException;

    /**
     * Reads the full file content as a string.
     *
     * @param fileName the name of the file to read
     * @param taskId   the task ID
     * @param type     the task type
     * @return the content of the file
     * @throws IOException if an I/O error occurs
     */
    String readFile(String fileName, int taskId, TaskType type) throws IOException;

    /**
     * Writes key-value entries to a file.
     *
     * @param entries  the list of entries to write
     * @param fileName the name of the file to write to
     * @param taskId   the task ID
     * @param type     the task type
     * @throws IOException if an I/O error occurs
     */
    void write(List<MapReduce.Entry> entries, String fileName, int taskId, TaskType type) throws IOException;

    /**
     * Clears the specified files for a given task.
     *
     * @param inputFiles list of file names to remove
     * @param taskId     the task ID
     * @param type       the task type
     * @throws IOException if an I/O error occurs
     */
    void clearFiles(List<String> inputFiles, int taskId, TaskType type) throws IOException;
}