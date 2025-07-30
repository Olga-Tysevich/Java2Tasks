package ru.java2.tasks.task4mapreduce.processor.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import ru.java2.tasks.task4mapreduce.inout.InOutHandler;
import ru.java2.tasks.task4mapreduce.processor.Coordinator;
import ru.java2.tasks.task4mapreduce.processor.Mapper;
import ru.java2.tasks.task4mapreduce.processor.Reducer;

/**
 * Configuration class for initializing a {@link Worker}.
 *
 * <p>Contains all required components for task execution:</p>
 * <ul>
 *     <li>{@link Coordinator} for task management</li>
 *     <li>{@link Mapper} for processing input files into intermediate key-value entries</li>
 *     <li>{@link Reducer} for aggregating and reducing grouped entries</li>
 *     <li>{@link InOutHandler} for file I/O operations</li>
 * </ul>
 */
@AllArgsConstructor
@Getter
@Setter
public class WorkerConf {
    private Coordinator coordinator;
    private Mapper mapper;
    private Reducer reducer;
    private InOutHandler handler;
}
