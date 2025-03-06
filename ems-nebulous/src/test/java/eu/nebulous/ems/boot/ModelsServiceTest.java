package eu.nebulous.ems.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.nebulous.ems.translate.TranslationService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

@Slf4j
class ModelsServiceTest {

    public static final String TESTS_YAML_FILE = "src/test/resources/ModelsServiceTest.yaml";

    public static final String COLOR_GREEN = "\u001b[32m";
    public static final String COLOR_YELLOW = "\u001b[33m";
    public static final String COLOR_RED = "\u001b[31m";
    public static final String COLOR_BLUE = "\u001b[34m";
    public static final String COLOR_MAGENTA = "\u001b[35m";
    public static final String COLOR_RESET = "\u001b[0m";

    private ObjectMapper objectMapper;
    private ModelsService modelsService;

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }

    @BeforeEach
    public void setUp() throws IOException {
        log.info("ModelsServiceTest: Setting up");
        objectMapper = new ObjectMapper();
        TranslationService translationService = new TranslationService(null, null);
        EmsBootProperties properties = new EmsBootProperties();
        properties.setModelsDir("target");
        properties.setModelsIndexFile("target/index.json");
        IndexService indexService = new IndexService(null, null, properties, objectMapper);
        modelsService = new ModelsService(translationService, properties, objectMapper, indexService);
        log.debug("ModelsServiceTest: modelsService: {}", modelsService);
        indexService.initIndexFile();
        log.debug("ModelsServiceTest: indexService initialized!!");
    }

    private void loadAndRunTests(String key, CheckedBiFunction<String,String,String> testRunner) throws IOException {
        try (InputStream inputStream = new FileInputStream(ModelsServiceTest.TESTS_YAML_FILE)) {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(inputStream);
            Object testData = data.get(key);
            if (testData==null) {
                log.warn(color("ModelsServiceTest: Key not found: {}", "EXCEPTION"), key);
                return;
            }

            int testNum = 1;
            if (testData instanceof Collection<?> testsList) {
                // If test data object is a Collection iterate through it and run each test item
                for (Object jsonObj : testsList) {
                    runTest(key, testNum, testRunner, jsonObj);
                    testNum++;
                }
            } else {
                // If test data object is NOT a Collection then run ONE test passing it the test data object
                runTest(key, testNum, testRunner, testData);
            }
        }
    }

    private void runTest(String key, int testNum, CheckedBiFunction<String, String, String> testRunner, Object jsonObj) {
        String testDescription = String.format("Test %s #%d", key, testNum);
        try {
            log.info(color("ModelsServiceTest:", "INFO"));
            log.info(color("ModelsServiceTest: ---------------------------------------------------------------", "INFO"));
            log.info(color("ModelsServiceTest: {}: json:\n{}", "INFO"), testDescription, jsonObj);
            String result = testRunner.apply(testDescription, jsonObj.toString());
            log.info(color("ModelsServiceTest: {}: Result: {}", result), testDescription, result);
        } catch (Exception e) {
            log.warn("ModelsServiceTest: {}: EXCEPTION: ", testDescription, e);
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

    @Test
    void extractBindings() throws IOException {
        loadAndRunTests("extractBindings", (testDescription, json) -> {
            Map body = objectMapper.readValue(json, Map.class);
            log.info("ModelsServiceTest: {}: body: {}", testDescription, body);

            Command command = new Command("key", "topic", body, null, null);
            String appId = body.getOrDefault("uuid", "").toString();
            return modelsService.extractBindings(command, appId);
        });
    }

    @Test
    void extractSolution() throws IOException {
        loadAndRunTests("extractSolution", (testDescription, json) -> {
            Map body = objectMapper.readValue(json, Map.class);
            log.info("ModelsServiceTest: {}: body: {}", testDescription, body);

            Command command = new Command("key", "topic", body, null, null);
            String appId = body.getOrDefault("uuid", "").toString();
            return modelsService.extractSolution(command, appId);
        });
    }
}