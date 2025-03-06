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
import java.util.List;
import java.util.Map;

@Slf4j
class ModelsServiceTest {

    private ObjectMapper objectMapper;
    private ModelsService modelsService;

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

    @Test
    void extractBindingsMany() throws IOException {
        try (InputStream inputStream = new FileInputStream("src/test/resources/ModelsServiceTest.yaml")) {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(inputStream);
            int testNum = 1;
            for (Object jsonObj : (List)data.getOrDefault("extractBindings", List.of())) {
                extractBindings(testNum, jsonObj.toString());
            }
        }
    }

    private void extractBindings(int testNum, String json) throws IOException {
        log.info("ModelsServiceTest: Test #{}: json:\n{}", testNum, json);

        Map body = objectMapper.readValue(json, Map.class);
        log.info("ModelsServiceTest: Test #{}: body: {}", testNum, body);

        Command command = new Command("key", "topic", body, null, null);
        String appId = body.getOrDefault("uuid", "").toString();
        String result = modelsService.extractBindings(command, appId);

        log.info("ModelsServiceTest: Test #{}: Result: {}", testNum, result);
    }
}