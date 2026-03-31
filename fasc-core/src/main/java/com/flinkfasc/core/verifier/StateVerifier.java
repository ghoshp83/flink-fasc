package com.flinkfasc.core.verifier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility for computing a deterministic SHA-256 hash of a Flink state value.
 *
 * <p>Used by the FASC coordinator to verify state consistency between App1 and App2
 * before initiating a handoff. Both apps process the same probe event, compute the
 * hash of the resulting state, and publish a {@link StateHashSignal}. The coordinator
 * compares hashes — a mismatch indicates non-deterministic business logic and the
 * handoff is aborted.
 *
 * <p><strong>Important:</strong> The hash is computed by serializing the state value
 * to canonical JSON (sorted keys). The state class must be Jackson-serializable and
 * must produce consistent output (no random fields, no timestamp fields using wall-clock time).
 */
public class StateVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(StateVerifier.class);

    private static final ObjectMapper CANONICAL_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);

    private StateVerifier() {}

    /**
     * Computes a SHA-256 hash of the given state value by serializing it to
     * canonical JSON and hashing the UTF-8 bytes.
     *
     * @param stateValue the Flink state object to hash (must be Jackson-serializable)
     * @return hex-encoded SHA-256 hash string, or {@code null} if serialization fails
     */
    public static String computeHash(Object stateValue) {
        if (stateValue == null) {
            return "null";
        }
        try {
            String json = CANONICAL_MAPPER.writeValueAsString(stateValue);
            return sha256Hex(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            LOG.warn("Failed to serialize state value for hashing: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Verifies that the hash of {@code actualStateValue} matches {@code expectedHash}.
     *
     * @param expectedHash     the hash published by the reference app (e.g. App1)
     * @param actualStateValue the state value from the app being verified (e.g. App2)
     * @return true if hashes match, false otherwise
     */
    public static boolean verify(String expectedHash, Object actualStateValue) {
        if (expectedHash == null) {
            LOG.warn("Expected hash is null — cannot verify state consistency");
            return false;
        }
        String actualHash = computeHash(actualStateValue);
        if (actualHash == null) {
            return false;
        }
        boolean match = expectedHash.equals(actualHash);
        if (!match) {
            LOG.error("State hash mismatch! expected={} actual={} — states have diverged. " +
                      "Check for non-deterministic business logic (wall-clock time, external calls).",
                      expectedHash, actualHash);
        }
        return match;
    }

    private static String sha256Hex(byte[] input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
}
