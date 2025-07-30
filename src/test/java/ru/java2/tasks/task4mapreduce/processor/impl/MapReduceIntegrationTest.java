package ru.java2.tasks.task4mapreduce.processor.impl;

import org.junit.jupiter.api.*;
import ru.java2.tasks.task4mapreduce.inout.InOutHandler;
import ru.java2.tasks.task4mapreduce.inout.impl.FileHandlerImpl;
import ru.java2.tasks.task4mapreduce.processor.Coordinator;
import ru.java2.tasks.task4mapreduce.utils.FaultyFileHandler;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the MapReduce framework. This test validates the MapReduce process,
 * including the word count task, both under normal conditions and with simulated failures.
 * <p>
 * The test runs the MapReduce job multiple times (as specified by the repetitions) and
 * verifies the results for correctness. It also includes scenarios where file I/O failures are simulated
 * (using {@link FaultyFileHandler}) to test the system's resilience to worker failures.
 * <p>
 * **Warnings:**
 * <ul>
 *   <li>For users running tests on less powerful computers, it is recommended to reduce the value of
 *       {@link #INPUT_DATA_REPETITION_MULTIPLIER} and the number of repetitions for tests
 *       to avoid long runtimes and potential timeouts.</li>
 *   <li>The test {@link #testWithFailuresRepeated} simulates worker failures. It may fail to wait for
 *       workers to finish within the timeout and result in a timeout failure.</li>
 * </ul>
 */
class MapReduceIntegrationTest {
    private static final int INPUT_DATA_REPETITION_MULTIPLIER = 50_000;
    private static final int NUM_WORKERS = 4;
    private static final int NUM_REDUCE_TASKS = 3;
    private static final List<String> INPUT_FILES = Arrays.asList(
            "file1.txt", "file2.txt", "file3.txt", "file4.txt"
    );

    private static Map<String, String> expectedResults;
    private static ExecutorService executor;
    private static Path testDir;

    @BeforeAll
    static void setup() throws Exception {
        testDir = Paths.get("target", "test_files");
        if (Files.exists(testDir)) {
            deleteDirectory(testDir);
        }
        Files.createDirectories(testDir);
    }

    @AfterAll
    static void tearDown() throws Exception {
        deleteDirectory(testDir);
    }

    @BeforeEach
    void setupEach() throws Exception {
        executor = Executors.newFixedThreadPool(NUM_WORKERS);
        createTestFiles();
    }

    @AfterEach
    void tearDownEach() throws IOException {
        executor.shutdownNow();
        if (Files.exists(testDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(testDir)) {
                for (Path file : stream) {
                    if (Files.isRegularFile(file)) {
                        Files.deleteIfExists(file);
                    }
                }
            }
        }
    }

    /**
     * Tests the word count MapReduce process, running it multiple times (as specified by the repetition count).
     * Verifies that the output matches the expected results.
     * <p>
     * This test does not simulate worker failures, and it verifies the correct word count in normal conditions.
     * </p>
     *
     * @param repetitionInfo Provides information about the current repetition count.
     */
    @RepeatedTest(value = 10, name = "{displayName} - повтор {currentRepetition}/{totalRepetitions}")
    void testWordCountMapReduceRepeated(RepetitionInfo repetitionInfo) throws Exception {
        System.out.println("Запуск №" + repetitionInfo.getCurrentRepetition());

        Coordinator coordinator = new CoordinatorImpl(
                INPUT_FILES,
                NUM_REDUCE_TASKS
        );

        InOutHandler fileHandler = new FileHandlerImpl();
        fileHandler.setOutDirectory(testDir.toAbsolutePath().toString());

        for (int i = 0; i < NUM_WORKERS; i++) {
            WorkerConf conf = new WorkerConf(
                    coordinator,
                    new WordCountMapper(),
                    new WordCountReducer(),
                    fileHandler
            );
            executor.execute(Worker.fromConf(conf, NUM_REDUCE_TASKS));
        }

        waitForCompletion(coordinator);
        validateResults();
    }

    /**
     * Tests the word count MapReduce process while simulating file I/O failures. This tests the system's resilience
     * to worker failures by causing read and write operations to fail periodically.
     * <p>
     * This test can experience timeouts if workers do not recover within the set task timeout.
     * </p>
     *
     * @param repetitionInfo Provides information about the current repetition count.
     */
    @RepeatedTest(value = 10, name = "{displayName} - повтор {currentRepetition}/{totalRepetitions}")
    void testWithFailuresRepeated(RepetitionInfo repetitionInfo) throws Exception {
        System.out.println("Запуск №" + repetitionInfo.getCurrentRepetition());

        Coordinator coordinator = new CoordinatorImpl(
                INPUT_FILES,
                NUM_REDUCE_TASKS
        );
        coordinator.setTaskTimeout(2_000);
        coordinator.setTaskCheckInterval(0);
        coordinator.setTaskCheckInterval(1);

        InOutHandler fileHandler = new FaultyFileHandler(1, 1);
        fileHandler.setOutDirectory(testDir.toAbsolutePath().toString());

        for (int i = 0; i < NUM_WORKERS; i++) {
            WorkerConf conf = new WorkerConf(
                    coordinator,
                    new WordCountMapper(),
                    new WordCountReducer(),
                    fileHandler
            );
            executor.execute(Worker.fromConf(conf, NUM_REDUCE_TASKS));
        }

        waitForCompletion(coordinator);
        validateResults();
    }

    private static void deleteDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {
                        }
                    });
        }
    }

    private void createTestFiles() throws Exception {
        Map<String, String> fileContents = new HashMap<>();
        fileContents.put("file1.txt", "apple banana orange apple");
        fileContents.put("file2.txt", "banana orange grape kiwi");
        fileContents.put("file3.txt", "apple banana melon");
        fileContents.put("file4.txt", "banana");


        for (String fileName : INPUT_FILES) {
            Path filePath = testDir.resolve(fileName);

            String baseContent = fileContents.get(fileName);
            try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                for (int i = 0; i < INPUT_DATA_REPETITION_MULTIPLIER; i++) {
                    writer.write(baseContent);
                    writer.newLine();
                }
            }
        }

        expectedResults = new HashMap<>();
        expectedResults.put("apple", String.valueOf(3 * INPUT_DATA_REPETITION_MULTIPLIER));
        expectedResults.put("banana", String.valueOf(4 * INPUT_DATA_REPETITION_MULTIPLIER));
        expectedResults.put("orange", String.valueOf(2 * INPUT_DATA_REPETITION_MULTIPLIER));
        expectedResults.put("grape", String.valueOf(INPUT_DATA_REPETITION_MULTIPLIER));
        expectedResults.put("kiwi", String.valueOf(INPUT_DATA_REPETITION_MULTIPLIER));
        expectedResults.put("melon", String.valueOf(INPUT_DATA_REPETITION_MULTIPLIER));
    }

    private void waitForCompletion(Coordinator coordinator) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (!coordinator.isDone()) {
            Thread.sleep(100);
            if (System.currentTimeMillis() - start > 30_000) {
                fail("Test timed out waiting for completion");
            }
        }
    }

    private void validateResults() throws Exception {
        Map<String, String> actualResults = new HashMap<>();
        for (int i = 0; i < NUM_REDUCE_TASKS; i++) {
            Path outputFile = testDir.resolve("mr-out-" + i);
            if (Files.exists(outputFile)) {
                parseOutputFile(outputFile, actualResults);
            }
        }

        assertEquals(expectedResults.size(), actualResults.size(),
                "Result size mismatch");

        expectedResults.forEach((word, expectedCount) -> {
            String actualCount = actualResults.get(word);
            assertNotNull(actualCount, "Missing word: " + word);
            assertEquals(expectedCount, actualCount,
                    "Incorrect count for word: " + word);
        });
    }

    private void parseOutputFile(Path file, Map<String, String> results) throws Exception {
        for (String line : Files.readAllLines(file)) {
            String[] parts = line.split("\\s+", 2);
            if (parts.length == 2) {
                results.put(parts[0], parts[1]);
            }
        }
    }
}
