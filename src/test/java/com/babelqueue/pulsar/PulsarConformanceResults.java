package com.babelqueue.pulsar;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.TestAbortedException;

/**
 * Records the outcome of every Pulsar conformance case and, when {@code BQ_CONFORMANCE_RESULTS} names a
 * path, writes a {@code results.json} in the format of {@code schema/results.schema.json} (ADR-0032 section 2)
 * when the test JVM exits. With the variable unset nothing is written.
 *
 * <p>This package carries the {@code pulsar} binding only, so exactly the manifest sections whose scope is
 * {@code pulsar} are in scope ({@code pulsar}, {@code pulsar_exhaustion}, {@code version_gate_pulsar}); the {@code core} sections are run and
 * reported by {@code babelqueue-core}. Every case id the manifest defines for an in-scope section ends up in the
 * file: a case that was never executed is written as {@code fail}, so a runner that forgets a case cannot go
 * green. The only {@code skip}s are the {@code pulsar_exhaustion} (ADR-0036 decision 1) and
 * {@code version_gate_pulsar} (ADR-0037 decision 1) rows with an {@code on_unknown_urn} strategy other than
 * {@code fail}, which this consumer does not have (the suite tells Java, .NET and Node runners to skip them).
 */
final class PulsarConformanceResults {

    static final String ENV_RESULTS = "BQ_CONFORMANCE_RESULTS";
    private static final String SDK = "babelqueue-java-pulsar";
    private static final String BINDING = "pulsar";

    /** section -> case id -> {status, reason}. */
    private static final Map<String, Map<String, String[]>> RECORDED = new ConcurrentHashMap<>();

    private static final Object INSTALL_LOCK = new Object();
    private static boolean installed;

    private PulsarConformanceResults() {
    }

    /**
     * Wraps {@code body}: a normal return is {@code pass}, a JUnit abort is {@code skip} (reason carries
     * the authorising ADR), anything else is {@code fail} and is rethrown.
     */
    static Executable record(String section, String id, Executable body) {
        return () -> {
            install();
            try {
                body.execute();
            } catch (TestAbortedException aborted) {
                skip(section, id, aborted.getMessage());
                throw aborted;
            } catch (Throwable failure) {
                put(section, id, "fail", String.valueOf(failure.getMessage()));
                throw failure;
            }
            put(section, id, "pass", null);
        };
    }

    /** Records {@code section}/{@code id} as skipped without running anything; {@code reason} cites its ADR. */
    static void skip(String section, String id, String reason) {
        install();
        put(section, id, "skip", reason);
    }

    private static void put(String section, String id, String status, String reason) {
        // A case can be exercised by several tests (payload_identity); a failure is never overwritten by a pass.
        RECORDED.computeIfAbsent(section, k -> new ConcurrentHashMap<>()).merge(id, new String[] {status, reason},
            (old, fresh) -> "fail".equals(old[0]) ? old : fresh);
    }

    private static void install() {
        String target = System.getenv(ENV_RESULTS);
        if (target == null || target.isBlank()) {
            return;
        }
        synchronized (INSTALL_LOCK) {
            if (installed) {
                return;
            }
            installed = true;
            Runtime.getRuntime().addShutdownHook(new Thread(PulsarConformanceResults::writeFile, "bq-conformance-results"));
        }
    }

    // ---- manifest enumeration -----------------------------------------------------------------

    /** The case-id rule of the conformance README ("Sections, scopes and exceptions"). */
    static List<String> caseIds(String section, Object body) {
        List<String> ids = new ArrayList<>();
        if (body instanceof JSONArray list) {
            addNamed(ids, "", section + "#", list);
            return ids;
        }
        if (!(body instanceof JSONObject map)) {
            return ids;
        }
        if (map.opt("cases") instanceof JSONArray list) {
            addNamed(ids, "", section + "#", list);
        }
        for (Iterator<String> keys = map.keys(); keys.hasNext();) {
            String key = keys.next();
            if ("description".equals(key) || "schema".equals(key) || "cases".equals(key)
                || !(map.get(key) instanceof JSONObject sub)) {
                continue;
            }
            if (sub.opt("cases") instanceof JSONArray list) {
                addNamed(ids, key + "/", "", list);
            } else {
                ids.add(key);
            }
        }
        return ids;
    }

    /** {@code fallbackPrefix} + the 0-based index names an unnamed case. */
    private static void addNamed(List<String> ids, String prefix, String fallbackPrefix, JSONArray list) {
        for (int i = 0; i < list.length(); i++) {
            Object item = list.get(i);
            String name = item instanceof JSONObject o && o.opt("name") instanceof String s ? s : null;
            ids.add(prefix + (name != null ? name : fallbackPrefix + i));
        }
    }

    /** section -> ordered case ids, for every manifest section whose scope is the {@code pulsar} binding. */
    static Map<String, List<String>> expectedCases(JSONObject manifest) {
        JSONObject scopes = manifest.getJSONObject("sections");
        Map<String, List<String>> expected = new java.util.LinkedHashMap<>();
        for (Iterator<String> keys = scopes.keys(); keys.hasNext();) {
            String section = keys.next();
            if (BINDING.equals(scopes.getJSONObject(section).getString("scope"))) {
                expected.put(section, caseIds(section, manifest.get(section)));
            }
        }
        return expected;
    }

    // ---- file ---------------------------------------------------------------------------------

    private static void writeFile() {
        writeTo(Path.of(System.getenv(ENV_RESULTS)), RECORDED);
    }

    /** Writes the results document for {@code recorded} to {@code target}; package-private so a test can exercise it. */
    static void writeTo(Path target, Map<String, Map<String, String[]>> recorded) {
        try {
            JSONObject manifest = new JSONObject(resource("/conformance/manifest.json"));
            JSONObject sections = new JSONObject(new java.util.LinkedHashMap<String, Object>());
            for (Map.Entry<String, List<String>> section : expectedCases(manifest).entrySet()) {
                Map<String, String[]> have = recorded.getOrDefault(section.getKey(), Map.of());
                JSONObject cases = new JSONObject(new java.util.LinkedHashMap<String, Object>());
                for (String id : section.getValue()) {
                    String[] outcome = have.get(id);
                    if (outcome == null) {
                        outcome = new String[] {"fail", "case was not executed by the runner"};
                    }
                    JSONObject result = new JSONObject(new java.util.LinkedHashMap<String, Object>());
                    result.put("status", outcome[0]);
                    if (outcome[1] != null && !"pass".equals(outcome[0])) {
                        result.put("reason", outcome[1].isBlank() ? "no detail" : outcome[1]);
                    }
                    cases.put(id, result);
                }
                sections.put(section.getKey(), cases);
            }
            JSONObject doc = new JSONObject(new java.util.LinkedHashMap<String, Object>());
            doc.put("sdk", SDK);
            doc.put("version", packageVersion());
            doc.put("conformance_version", resource("/conformance/CONFORMANCE_VERSION").strip());
            doc.put("generated_at", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
            doc.put("bindings", new JSONArray(List.of(BINDING)));
            doc.put("sections", sections);
            Files.writeString(target, doc.toString(2) + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The package version from the build's own {@code pom.xml}: the first {@code <version>} after the artifactId. */
    static String packageVersion() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"), StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("<artifactId>babelqueue-pulsar</artifactId>\\s*<version>([^<]+)</version>").matcher(pom);
        if (!m.find()) {
            throw new IllegalStateException("cannot read the package version from pom.xml");
        }
        return m.group(1).strip();
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = PulsarConformanceResults.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
