package com.flinkfasc.core.control;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Stateless serialiser / deserialiser for {@link ControlSignal} objects.
 *
 * <p>Wire format is UTF-8 encoded JSON. The underlying {@link ObjectMapper} is created
 * once and is thread-safe after construction; a single {@code ControlSignalSerde} instance
 * may therefore be shared across threads (e.g. between the producer and consumer threads
 * inside {@link com.flinkfasc.core.ShadowSink}).
 *
 * <h3>Error handling</h3>
 * Both {@link #serialize} and {@link #deserialize} wrap checked Jackson exceptions in an
 * unchecked {@link ControlSignalSerdeException} (a {@link RuntimeException} subclass) so
 * that callers do not need to handle checked exceptions in hot-path streaming code.
 */
public class ControlSignalSerde {

    /** Thread-safe Jackson mapper shared by all instances. */
    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        // Avoid failures when the JVM's ZoneId changes; timestamps are stored as epoch millis
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Do not fail on empty beans — keeps compatibility with subclasses
        MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    // -------------------------------------------------------------------------
    // Serialisation
    // -------------------------------------------------------------------------

    /**
     * Serialises the given {@link ControlSignal} to a UTF-8 encoded JSON byte array.
     *
     * @param signal the signal to serialise; must not be {@code null}
     * @return UTF-8 JSON bytes
     * @throws ControlSignalSerdeException if Jackson encounters a serialisation error
     */
    public byte[] serialize(ControlSignal signal) {
        if (signal == null) {
            throw new IllegalArgumentException("ControlSignalSerde.serialize: signal must not be null");
        }
        try {
            return MAPPER.writeValueAsBytes(signal);
        } catch (JsonProcessingException e) {
            throw new ControlSignalSerdeException(
                    "Failed to serialise ControlSignal of type " + signal.getSignalType(), e);
        }
    }

    /**
     * Convenience overload that returns the JSON as a {@link String} rather than a
     * {@code byte[]}.  Useful for logging / debugging.
     *
     * @param signal the signal to serialise; must not be {@code null}
     * @return JSON string
     * @throws ControlSignalSerdeException if Jackson encounters a serialisation error
     */
    public String serializeToString(ControlSignal signal) {
        return new String(serialize(signal), StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Deserialisation
    // -------------------------------------------------------------------------

    /**
     * Deserialises a {@link ControlSignal} from a UTF-8 encoded JSON byte array.
     *
     * @param bytes the raw Kafka message value; must not be {@code null}
     * @return the deserialised {@link ControlSignal}
     * @throws ControlSignalSerdeException if the bytes cannot be parsed as a valid
     *         {@link ControlSignal}
     */
    public ControlSignal deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException(
                    "ControlSignalSerde.deserialize: bytes must not be null or empty");
        }
        try {
            return MAPPER.readValue(bytes, ControlSignal.class);
        } catch (IOException e) {
            throw new ControlSignalSerdeException(
                    "Failed to deserialise ControlSignal from bytes (length=" + bytes.length + ")", e);
        }
    }

    /**
     * Convenience overload that accepts a JSON string instead of a raw byte array.
     *
     * @param json the JSON string; must not be {@code null} or blank
     * @return the deserialised {@link ControlSignal}
     * @throws ControlSignalSerdeException if the string cannot be parsed
     */
    public ControlSignal deserialize(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                    "ControlSignalSerde.deserialize: json string must not be null or blank");
        }
        return deserialize(json.getBytes(StandardCharsets.UTF_8));
    }

    // =========================================================================
    // Exception type
    // =========================================================================

    /**
     * Unchecked exception thrown when a serialisation or deserialisation operation fails.
     * Wraps the underlying {@link JsonProcessingException} or {@link IOException}.
     */
    public static class ControlSignalSerdeException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /** Constructs the exception with a message and the underlying cause. */
        public ControlSignalSerdeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
