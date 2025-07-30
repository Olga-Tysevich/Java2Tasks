package ru.java2.tasks.task4mapreduce.validator.impl;

import lombok.extern.slf4j.Slf4j;
import ru.java2.tasks.task4mapreduce.exceptions.ValidationException;
import ru.java2.tasks.task4mapreduce.validator.Validator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Default implementation of {@link Validator}.
 *
 * @param <T> type of object to validate
 */
@Slf4j
public class ValidatorImpl<T> implements Validator<T> {
    private final Map<String, Rule<T>> rules = new HashMap<>();


    @Override
    public ValidatorImpl<T> addRule(String name, Predicate<T> condition, Function<T, String> errorMessage) {
        rules.put(name, new Rule<>(condition, errorMessage));
        return this;
    }

    @Override
    public ValidatorImpl<T> removeRule(String name) {
        rules.remove(name);
        return this;
    }

    @Override
    public List<String> validate(T object) {
        log.info("Starting validation for object: {}", object);
        List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Rule<T>> entry : rules.entrySet()) {
            String ruleName = entry.getKey();
            Rule<T> rule = entry.getValue();
            if (!rule.condition.test(object)) {
                String error = rule.errorMessage.apply(object);
                log.warn("Validation failed for rule '{}': {}", ruleName, error);
                errors.add(error);
            }
        }
        log.info("Validation completed. Total errors: {}", errors.size());
        return errors;
    }

    @Override
    public void validateAndThrow(T object) {
        log.info("Performing validateAndThrow for object: {}", object);
        List<String> errors = validate(object);
        if (!errors.isEmpty()) {
            log.error("Validation failed. Throwing ValidationException with errors: {}", errors);
            throw new ValidationException(errors);
        }
        log.info("Validation passed with no errors.");
    }

    private record Rule<T>(Predicate<T> condition, Function<T, String> errorMessage) {
    }
}