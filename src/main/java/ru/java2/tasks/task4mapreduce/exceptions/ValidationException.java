package ru.java2.tasks.task4mapreduce.exceptions;

import lombok.Getter;

import java.util.List;

/**
 * Exception thrown when object validation fails.
 */
@Getter
public class ValidationException extends RuntimeException {
    /**
     * List of validation errors.
     */
    private final List<String> errors;

    public ValidationException(List<String> errors) {
        super("Validation failed: " + String.join(", ", errors));
        this.errors = errors;
    }

}
