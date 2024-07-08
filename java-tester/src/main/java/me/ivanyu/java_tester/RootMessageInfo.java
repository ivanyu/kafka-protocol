/**
 * Taken from https://github.com/Aiven-Open/kio
 */

package me.ivanyu.java_tester;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class RootMessageInfo {
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        OBJECT_MAPPER = new ObjectMapper(jsonFactory);
    }

    final short version;
    final Schema rootSchema;
    final Map<String, JsonNode> commonStructs;
    final EntityClass<ApiMessage> rootClazz;

    RootMessageInfo(String shortClassName, short version) throws Exception {
        this.version = version;

        JsonNode schemaJson;
        String fileName = shortClassName;
        if (shortClassName.equals("DefaultPrincipal")) {
            fileName += "Data";
        }
        String schemaResource = "common/message/" + fileName + ".json";
        try (InputStream resource = RootMessageInfo.class.getClassLoader().getResourceAsStream(schemaResource)) {
            schemaJson = OBJECT_MAPPER.readTree(resource);
        }

        this.commonStructs = new HashMap<>();
        if (schemaJson.get("commonStructs") != null) {
            Iterator<JsonNode> elements = schemaJson.get("commonStructs").elements();
            while (elements.hasNext()) {
                JsonNode struct = elements.next();
                commonStructs.put(struct.get("name").asText(), struct);
            }
        }

        this.rootSchema = new Schema(schemaJson, commonStructs);

        rootClazz = getRootClass(shortClassName);
    }

    private static EntityClass<ApiMessage> getRootClass(String shortClassName) throws ClassNotFoundException {
        String className = "org.apache.kafka.common.message." + shortClassName;
        if (!shortClassName.equals("SnapshotFooterRecord")
            && !shortClassName.equals("SnapshotHeaderRecord")
            && !shortClassName.equals("ConsumerProtocolAssignment")
            && !shortClassName.equals("ConsumerProtocolSubscription")
            && !shortClassName.equals("LeaderChangeMessage")
            && !shortClassName.equals("DefaultPrincipalData")
            && !shortClassName.equals("VotersRecord")
            && !shortClassName.equals("KRaftVersionRecord")
        ) {
            className += "Data";
        }
        @SuppressWarnings("unchecked")
        Class<ApiMessage> clazz = (Class<ApiMessage>) RootMessageInfo.class.getClassLoader().loadClass(className);
        return new EntityClass<>(clazz);
    }
}
