package com.babelqueue.pulsar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The {@code BQ_CONFORMANCE_RESULTS} writer, exercised against a private recorded-map and a temp file. */
class PulsarConformanceResultsTest {

    private static JSONObject manifest() throws Exception {
        try (InputStream in = PulsarConformanceResultsTest.class.getResourceAsStream("/conformance/manifest.json")) {
            return new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void inScopeSectionsAreExactlyTheManifestsPulsarScopedOnes() throws Exception {
        Map<String, List<String>> expected = PulsarConformanceResults.expectedCases(manifest());
        assertEquals(java.util.Set.of("pulsar", "pulsar_exhaustion", "version_gate_pulsar"), expected.keySet());
        assertTrue(expected.get("pulsar").contains("property_projection"));
        assertTrue(expected.get("pulsar").contains("schema_version_gate/0"));
        assertTrue(expected.get("pulsar_exhaustion").contains("failed-reconciled-2-dlq-on-dead-letters"));
        assertEquals(17, expected.get("version_gate_pulsar").size());
        assertTrue(expected.get("version_gate_pulsar").contains("default-dlq-on-write-ok"));
    }

    @Test
    void writesAResultsFileWithSkipRecordsAndUnexecutedCasesAsFail(@TempDir Path dir) throws Exception {
        Map<String, List<String>> expected = PulsarConformanceResults.expectedCases(manifest());
        String section = "pulsar_exhaustion";
        String skipped = expected.get(section).get(0);
        Map<String, Map<String, String[]>> recorded = new ConcurrentHashMap<>();
        recorded.computeIfAbsent(section, k -> new ConcurrentHashMap<>())
            .put(skipped, new String[] {"skip", "ADR-0036: no on_unknown_urn option"});

        Path target = dir.resolve("results.json");
        PulsarConformanceResults.writeTo(target, recorded);

        JSONObject doc = new JSONObject(Files.readString(target, StandardCharsets.UTF_8));
        assertEquals("babelqueue-java-pulsar", doc.getString("sdk"));
        assertEquals(PulsarConformanceResults.packageVersion(), doc.getString("version"));
        assertEquals("pulsar", doc.getJSONArray("bindings").getString(0));
        assertFalse(doc.getString("conformance_version").isBlank());

        JSONObject cases = doc.getJSONObject("sections").getJSONObject(section);
        assertEquals(expected.get(section).size(), cases.length());
        assertEquals("skip", cases.getJSONObject(skipped).getString("status"));
        assertEquals("ADR-0036: no on_unknown_urn option", cases.getJSONObject(skipped).getString("reason"));
        // A case the runner never recorded is a failure, not a silent omission.
        assertEquals("fail", cases.getJSONObject(expected.get(section).get(1)).getString("status"));
        assertEquals(expected.get("pulsar").size(), doc.getJSONObject("sections").getJSONObject("pulsar").length());
    }

    @Test
    void passRecordsCarryNoReason(@TempDir Path dir) throws Exception {
        Map<String, Map<String, String[]>> recorded = new ConcurrentHashMap<>();
        recorded.computeIfAbsent("pulsar", k -> new ConcurrentHashMap<>()).put("property_projection", new String[] {"pass", null});

        Path target = dir.resolve("results.json");
        PulsarConformanceResults.writeTo(target, recorded);

        JSONObject result = new JSONObject(Files.readString(target, StandardCharsets.UTF_8))
            .getJSONObject("sections").getJSONObject("pulsar").getJSONObject("property_projection");
        assertEquals("pass", result.getString("status"));
        assertFalse(result.has("reason"));
    }
}
