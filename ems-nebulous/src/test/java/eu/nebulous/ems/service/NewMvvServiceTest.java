package eu.nebulous.ems.service;

import eu.nebulous.ems.AbstractBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
class NewMvvServiceTest extends AbstractBaseTest {

    public static final String TESTS_YAML_FILE = "src/test/resources/MvvServiceTest.yaml";

    private MvvService mvvService;

    @BeforeAll
    public void setUp() throws IOException {
        log.info("MvvServiceTest: Setting up");
        mvvService = new MvvService(null);
    }

    static List<Arguments> testsForTranslateAndSetControlServiceConstants() throws IOException {
        return getTestsFromYamlFile(NewMvvServiceTest.class, TESTS_YAML_FILE, "translateAndSetControlServiceConstants");
    }

    @ParameterizedTest
    @MethodSource("testsForTranslateAndSetControlServiceConstants")
    void translateAndSetControlServiceConstants(String testDescription, Object yamlObj, String expectedOutcome) throws IOException {
        log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        log.warn("testDescription: {}", testDescription);
        log.warn("yamlObj: {}", yamlObj);
        log.warn("expectedOutcome: {}", expectedOutcome);

        Map testData = toMap(yamlObj);
        log.debug("translateAndSetControlServiceConstants: {}: testData: {}", testDescription, testData);

        Map bindings = toMap(Objects.requireNonNullElse(testData.get("bindings"), Map.of()));
        Map newValues = toMap(Objects.requireNonNullElse(testData.get("solution"), Map.of()));

        MvvService mvvService = new MvvService(null);
        mvvService.setBindings(bindings);
        log.info("translateAndSetControlServiceConstants: values BEFORE: {}", mvvService.getValues());
        try {
            mvvService.translateAndSetValues(newValues);
        } catch (Exception e) {
            log.warn("translateAndSetControlServiceConstants: EXCEPTION: ", e);
        }
        Map<String, Double> updatedMvvValues = mvvService.getValues();
        log.info("translateAndSetControlServiceConstants: values AFTER: {}", updatedMvvValues);

        if (StringUtils.isNotBlank(expectedOutcome) && updatedMvvValues!=null)
            Assertions.assertEquals(expectedOutcome, updatedMvvValues.toString());
    }
}