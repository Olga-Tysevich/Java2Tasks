package ru.java2.tasks.task4mapreduce.utils;

public interface Constants {
    String MAP_TASK_NAME_PATTERN = "mr-%d-%d";
    String REDUCE_TASK_NAME = "mr-out-";
    String REDUCE_TASK_NAME_PATTERN = REDUCE_TASK_NAME + "%d";
}
