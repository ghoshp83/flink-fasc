package com.flinkfasc.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserializes Kafka messages (JSON bytes) into {@link BusinessEvent} objects.
 *
 * <p>Expected JSON format:
 * <pre>
 * {
 *   "eventId":        "evt-001",
 *   "customerId":     "cust-123",
 *   "eventType":      "ORDER_CREATED",
 *   "amount":         99.99,
 *   "eventTimestamp": 1711700000000,
 *   "payload":        "{\"orderId\":\"ord-456\"}"
 * }
 * </pre>
 */
public class BusinessEventDeserializer implements DeserializationSchema<BusinessEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public BusinessEvent deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, BusinessEvent.class);
    }

    @Override
    public boolean isEndOfStream(BusinessEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<BusinessEvent> getProducedType() {
        return TypeInformation.of(BusinessEvent.class);
    }
}
