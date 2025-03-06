package eu.nebulous.ems;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

@Slf4j
public abstract class AbstractBaseTest {

    public static final String COLOR_GREEN = "\u001b[32m";
    public static final String COLOR_YELLOW = "\u001b[33m";
    public static final String COLOR_RED = "\u001b[31m";
    public static final String COLOR_BLUE = "\u001b[34m";
    public static final String COLOR_MAGENTA = "\u001b[35m";
    public static final String COLOR_RESET = "\u001b[0m";

    protected ObjectMapper objectMapper = new ObjectMapper();

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    protected void loadAndRunTests(@NonNull Object caller, @NonNull String key, @NonNull String testsFile, @NonNull CheckedBiFunction<String, String, String> testRunner) throws IOException {
        try (InputStream inputStream = new FileInputStream(testsFile)) {
            String callerClass = caller.getClass().getSimpleName();
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(inputStream);
            Object testData = data.get(key);
            if (testData==null) {
                log.warn(color("{}: Key not found: {}", "EXCEPTION"), callerClass, key);
                return;
            }

            int testNum = 1;
            if (testData instanceof Collection<?> testsList) {
                // If test data object is a Collection iterate through it and run each test item
                for (Object jsonObj : testsList) {
                    runTest(callerClass, key, testNum, testRunner, jsonObj);
                    testNum++;
                }
            } else {
                // If test data object is NOT a Collection then run ONE test passing it the test data object
                runTest(callerClass, key, testNum, testRunner, testData);
            }
        }
    }

    private void runTest(String callerClass, String key, int testNum, CheckedBiFunction<String, String, String> testRunner, Object jsonObj) {
        String testDescription = String.format("Test %s #%d", key, testNum);
        try {
            log.info(color("{}:", "INFO"), callerClass);
            log.info(color("{}: ---------------------------------------------------------------", "INFO"), callerClass);
            log.info(color("{}: {}: json:\n{}", "INFO"), callerClass, testDescription, jsonObj);
            String result = testRunner.apply(testDescription, jsonObj.toString());
            log.info(color("{}: {}: Result: {}", result), callerClass, testDescription,
                    Objects.requireNonNullElse(result, color("(no result returned)", "WARN")));
        } catch (Exception e) {
            log.warn("{}: {}: EXCEPTION: ", callerClass, testDescription, e);
        }
    }

    private String color(String message, String result) {
        if (result==null) result = "";
        result = result.split("[^\\p{Alnum}]", 2)[0].toUpperCase();
        String color = null;
        switch (result) {
            case "OK" -> color = COLOR_GREEN;
            case "ERROR" -> color = COLOR_YELLOW;
            case "EXCEPTION" -> color = COLOR_RED;
            case "INFO" -> color = COLOR_BLUE;
            case "WARN" -> color = COLOR_MAGENTA;
        }
        return color==null
                ? message
                : color + message + COLOR_RESET;
    }
}