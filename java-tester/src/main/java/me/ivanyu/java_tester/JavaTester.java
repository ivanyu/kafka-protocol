/**
 * Taken from https://github.com/Aiven-Open/kio
 */

package me.ivanyu.java_tester;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JavaTester {
    private static final String SUCCESS_RESPONSE = "{\"success\": true}";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String CMD_PRINT_ERROR_CODES = "--print-error-codes";

    public static void main(String[] args) throws Exception {
        if (Arrays.stream(args).anyMatch(CMD_PRINT_ERROR_CODES::equals)) {
            System.err.println("Printing error codes");

            for (Errors error : Errors.values()) {
                System.out.print(error.code());
                System.out.print(" ");
                System.out.print(error.name());
                System.out.print(" ");
                // https://github.com/apache/kafka/blob/84b2d5bedf4381ebfcf32a4671f096de937beb31/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L551
                System.out.print(
                    error.exception() != null && error.exception() instanceof RetriableException ? "True" : "False"
                );
                System.out.print(" ");
                System.out.println(error.message());
            }

            return;
        }

        System.out.println("Java tester started");
        System.out.flush();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String caseStr;
            while ((caseStr = reader.readLine()) != null) {
                String result = test(caseStr);
                System.out.println(result);
                System.out.flush();
            }
        }
    }

    private static String test(String caseStr) {
        try {
            JsonNode caseNode = OBJECT_MAPPER.readTree(caseStr);
            String testType = caseNode.get("testType").asText();
            if (testType.equals("default")) {
                return testDefault(caseStr, caseNode);
            } else if (testType.equals("arbitrary")) {
                return testArbitrary(caseStr, caseNode);
            } else {
                throw new Exception("Unknown test type: " + testType);
            }
        } catch (Exception e) {
            return exceptionResponse(e, caseStr);
        }
    }

    private static String testDefault(String caseStr, JsonNode caseNode) throws Exception {
        short version = caseNode.get("version").shortValue();
        String shortClassName = caseNode.get("class").asText();
        RootMessageInfo rootMessageInfo = new RootMessageInfo(shortClassName, version);

        ApiMessage constructedMessage =
                new ObjectCreator<>(rootMessageInfo, rootMessageInfo.rootClazz, rootMessageInfo.rootSchema)
                        .createDefault();

        byte[] serializedFromExternal = Base64.getDecoder().decode(caseNode.get("serialized").asText());
        ApiMessage messageDeserializedFromExternal = deserialize(rootMessageInfo, version, serializedFromExternal);
        byte[] serializedInJava = serialize(constructedMessage, version);

        if (!messageDeserializedFromExternal.equals(constructedMessage)) {
            String message = "Deserialized message is not equal to constructed\n"
                    + "Input: " + caseStr + "\n"
                    + "Deserialized: " + messageDeserializedFromExternal + "\n"
                    + "Constructed: " + constructedMessage;
            return failureResponse(message);
        } else {
            if (!Arrays.equals(serializedFromExternal, serializedInJava)) {
                String message = "Message serialized in Java is not equal to message externally serialized\n"
                        + "Input: " + caseStr + "\n"
                        + "Deserialized: " + messageDeserializedFromExternal + "\n"
                        + "Constructed: " + constructedMessage;
                return failureResponse(message);
            } else {
                return SUCCESS_RESPONSE;
            }
        }
    }

    private static String testArbitrary(String caseStr, JsonNode caseNode) throws Exception {
        short version = caseNode.get("version").shortValue();
        String shortClassName = caseNode.get("class").asText();
        RootMessageInfo rootMessageInfo = new RootMessageInfo(shortClassName, version);

        ApiMessage constructedMessage =
                new ObjectCreator<>(rootMessageInfo, rootMessageInfo.rootClazz, rootMessageInfo.rootSchema)
                        .create(caseNode.get("json"));

        byte[] serializedFromExternal = Base64.getDecoder().decode(caseNode.get("serialized").asText());
        ApiMessage messageDeserializedFromExternal = deserialize(rootMessageInfo, version, serializedFromExternal);
        byte[] serializedInJava = serialize(constructedMessage, version);

        if (!messageDeserializedFromExternal.equals(constructedMessage)) {
            String message = "Deserialized message is not equal to constructed\n"
                    + "Input: " + caseStr + "\n"
                    + "Deserialized: " + messageDeserializedFromExternal + "\n"
                    + "Constructed: " + constructedMessage;
            return failureResponse(message);
        } else {
            if (!Arrays.equals(serializedFromExternal, serializedInJava)) {
                String message = "Message serialized in Java is not equal to message externally serialized\n"
                        + "Input: " + caseStr + "\n"
                        + "Deserialized: " + messageDeserializedFromExternal + "\n"
                        + "Constructed: " + constructedMessage;
                return failureResponse(message);
            } else {
                return SUCCESS_RESPONSE;
            }
        }
    }

    private static String failureResponse(String message) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put("success", false);
        objectNode.put("message", message);
        try {
            return OBJECT_MAPPER.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            // this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    private static String exceptionResponse(Exception exception, String caseStr) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put("success", false);

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        exception.printStackTrace(printWriter);
        objectNode.put("exception", stringWriter.toString() + "\n" + "Case: " + caseStr);

        try {
            return OBJECT_MAPPER.writeValueAsString(objectNode);
        } catch (JsonProcessingException e) {
            // this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    private static ApiMessage deserialize(RootMessageInfo rootMessageInfo, short version, byte[] serializedFromExternal) throws Exception {
        Readable readable = new ByteBufferAccessor(ByteBuffer.wrap(serializedFromExternal));
        ApiMessage messageDeserializedFromExternal = rootMessageInfo.rootClazz.newInstance();
        messageDeserializedFromExternal.read(readable, version);
        return messageDeserializedFromExternal;
    }

    private static byte[] serialize(ApiMessage message, short version) {
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        int size = message.size(objectSerializationCache, version);
        ByteBufferAccessor writer = new ByteBufferAccessor(ByteBuffer.allocate(size));
        message.write(writer, objectSerializationCache, version);
        return writer.buffer().array();
    }
}
