package ru.java2.tasks.task4mapreduce.validator;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
/**
 * Generic validator interface for validating objects with multiple rules.
 *
 * @param <T> type of object to validate
 */
public interface Validator<T> {

    /**
     * Adds a validation rule.
     *
     * @param name the rule name
     * @param condition the predicate representing the validation condition
     * @param errorMessage function to generate an error message
     * @return the current validator
     */
    Validator<T> addRule(String name, Predicate<T> condition, Function<T, String> errorMessage);

    /**
     * Removes a validation rule by name.
     *
     * @param name the rule name
     * @return the current validator
     */
    Validator<T> removeRule(String name);

    /**
     * Validates an object and returns a list of errors.
     *
     * @param object the object to validate
     * @return list of validation errors
     */
    List<String> validate(T object);

    /**
     * Validates an object and throws {@link ru.java2.tasks.task4mapreduce.exceptions.ValidationException} if validation fails.
     *
     * @param object the object to validate
     * @throws  ru.java2.tasks.task4mapreduce.exceptions.ValidationException if validation fails
     */
    void validateAndThrow(T object);
}