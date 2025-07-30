package ru.java2.tasks.task4mapreduce.validator.impl;

import lombok.experimental.UtilityClass;
import ru.java2.tasks.task4mapreduce.domain.Task;
import ru.java2.tasks.task4mapreduce.domain.TaskStatus;
import ru.java2.tasks.task4mapreduce.domain.TaskType;
import ru.java2.tasks.task4mapreduce.processor.impl.WorkerConf;
import ru.java2.tasks.task4mapreduce.validator.Validator;

import java.util.List;

/**
 * Factory class for creating commonly used validators.
 */
@UtilityClass
public class ValidatorFactory {

    public static Validator<Task> taskValidator() {
        return new ValidatorImpl<Task>().addRule(
                        "id-non-negative",
                        t -> TaskType.FINISH == t.getType() || t.getId() >= 0,
                        t -> "Task ID cannot be negative: " + t.getId()
                )
                .addRule(
                        "type-not-null",
                        t -> t.getType() != null,
                        t -> "Task type cannot be null"
                )
                .addRule(
                        "status-not-null-and-non-idle-and-non-completed",
                        t -> TaskType.FINISH == t.getType() || t.getStatus() != null && t.getStatus() != TaskStatus.IDLE && t.getStatus() != TaskStatus.COMPLETED,
                        t -> "Task type cannot be null"
                )
                .addRule(
                        "task-input-files-validation",
                        t -> TaskType.FINISH == t.getType()
                                || (t.getType() == TaskType.MAP && (t.getInputFiles() != null && !t.getInputFiles().isEmpty()))
                                || (t.getType() == TaskType.REDUCE && (t.getInputFiles() != null && !t.getInputFiles().isEmpty())),
                        t -> "Task input files is invalid. Task id: " + t.getId() + " type: " + t.getType() + " status: " + t.getStatus()
                                + " input files: " + t.getInputFiles() + " output files: " + t.getAllOutputFiles()
                );
    }

    public static Validator<WorkerConf> workerConfValidator() {
        return new ValidatorImpl<WorkerConf>()
                .addRule(
                        "coordinator-not-null",
                        c -> c.getCoordinator() != null,
                        c -> "Coordinator cannot be null"
                )
                .addRule(
                        "mapper-not-null",
                        c -> c.getMapper() != null,
                        c -> "Mapper cannot be null"
                )
                .addRule(
                        "reducer-not-null",
                        c -> c.getReducer() != null,
                        c -> "Reducer cannot be null"
                )
                .addRule(
                        "handler-not-null",
                        c -> c.getHandler() != null,
                        c -> "Handler cannot be null"
                );
    }

    public static Validator<List<?>> listNotEmptyValidator(String message) {
        return new ValidatorImpl<List<?>>()
                .addRule(
                        "list-not-empty",
                        l -> l != null && !l.isEmpty(),
                        l -> message
                );
    }

    public static Validator<Integer> positiveIntValidator(String message) {
        return new ValidatorImpl<Integer>()
                .addRule(
                        "positive-int",
                        i -> i > 0,
                        l -> message
                );
    }

}
