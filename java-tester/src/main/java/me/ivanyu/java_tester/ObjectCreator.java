/**
 * Taken from https://github.com/Aiven-Open/kio
 */

package me.ivanyu.java_tester;

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.text.CaseUtils;

class ObjectCreator<T> extends BaseCreator {
    private final EntityClass<T> clazz;
    private final Schema schema;

    ObjectCreator(RootMessageInfo rootMessageInfo, EntityClass<T> clazz, Schema schema) {
        super(rootMessageInfo);
        this.clazz = clazz;
        this.schema = schema;
    }

    T createDefault() throws Exception {
        return clazz.newInstance();
    }

    T create(JsonNode json) throws Exception {
        if (json.isNull()) {
            return null;
        }

        T instance = clazz.newInstance();

        List<String> knownFieldNames = schema.knownFieldNames();

        Iterator<String> fieldNames = json.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();

            // Skip unknown tags -- we don't test them now
            if (fieldName.equals("_unknown_tagged_fields")) {
                if (!json.get(fieldName).isArray() || !json.get(fieldName).isEmpty()) {
                    throw new Exception("Non-empty array _unknown_tagged_fields are not supported");
                }
                continue;
            }

            String kafkaesqueFieldName = kafkaesqueFieldName(fieldName, knownFieldNames);
            Schema fieldSchema = schema.fieldSchema(kafkaesqueFieldName);
            JsonNode fieldValue = json.get(fieldName);
            Setter setter = clazz.setterForFieldName(kafkaesqueFieldName);
            Class<?> parameterType = setter.parameterType();

            if (fieldValue == null) {
                if (parameterType.isPrimitive()) {
                    throw new Exception("The parameter is primitive (" + parameterType + "), " +
                        "but value of field " + fieldName + " is null");
                }
                setter.invoke(instance, (Object) null);
            } else if (parameterType.equals(byte.class)) {
                setter.invoke(instance, getByte(fieldValue, fieldName));
            } else if (parameterType.equals(short.class)) {
                if (fieldValue.isNull()) {
                    if (fieldSchema.isTaggedVersion(rootMessageInfo.version)) {
                        // Do nothing for the tagged version.
                        continue;
                    } else {
                        throw new Exception("Unexpected null for non-tagged field " + fieldName);
                    }
                }
                setter.invoke(instance, getShort(fieldValue, fieldName));
            } else if (parameterType.equals(int.class)) {
                setter.invoke(instance, getInt(fieldValue, fieldName));
            } else if (parameterType.equals(long.class)) {
                setter.invoke(instance, getLong(fieldValue, fieldName));
            } else if (parameterType.equals(double.class)) {
                setter.invoke(instance, getDouble(fieldValue, fieldName));
            } else if (parameterType.equals(boolean.class)) {
                setter.invoke(instance, getBoolean(fieldValue, fieldName));
            } else if (parameterType.isPrimitive()) {
                // Add the handling fo this type if you face this.
                throw new Exception("Unsupported primitive type " + parameterType);
            } else if (parameterType.isArray()) {
                if (parameterType.getComponentType().equals(byte.class)) {
                    setter.invoke(instance, (Object) getBytes(fieldValue, fieldName));
                } else {
                    throw new Exception("Unsupported array type " + parameterType);
                }
            } else if (parameterType.equals(String.class)) {
                setter.invoke(instance, getString(fieldValue, fieldName));
            } else if (parameterType.equals(Uuid.class)) {
                Uuid uuid = getUuid(fieldValue, fieldName);
                if (uuid != null) {
                    setter.invoke(instance, uuid);
                }
            } else if (parameterType.equals(ByteBuffer.class)) {
                setter.invoke(instance, getByteBuffer(fieldValue, fieldName));
            } else if (AbstractCollection.class.isAssignableFrom(parameterType)) {
                CollectionCreator creator = new CollectionCreator(rootMessageInfo, fieldValue, fieldName, fieldSchema);
                @SuppressWarnings("unchecked")
                Class<AbstractCollection<Object>> parameterTypeCollection = (Class<AbstractCollection<Object>>) parameterType;
                AbstractCollection<Object> collection = creator.createAbstractCollection(parameterTypeCollection);
                setter.invoke(instance, collection);
            } else if (List.class.isAssignableFrom(parameterType)) {
                List<?> list = new CollectionCreator(rootMessageInfo, fieldValue, fieldName, fieldSchema).createList();
                setter.invoke(instance, list);
            } else if (BaseRecords.class.isAssignableFrom(parameterType)) {
                ByteBuffer buffer = getByteBuffer(fieldValue, fieldName);
                if (buffer != null) {
                    setter.invoke(instance, MemoryRecords.readableRecords(buffer));
                }
            } else {
                Object o = new ObjectCreator<>(
                    rootMessageInfo, new EntityClass<>(parameterType), fieldSchema).create(fieldValue);
                setter.invoke(instance, o);
            }
        }

        return instance;
    }

    private static String kafkaesqueFieldName(String fieldName, List<String> knownFieldNames) {
        switch (fieldName) {
            case "timeout":
                fieldName = "timeout_ms";
                break;
            case "throttle_time":
                fieldName = "throttle_time_ms";
                break;
            case "max_wait":
                fieldName = "max_wait_ms";
                break;
            case "session_lifetime":
                fieldName = "session_lifetime_ms";
                break;
            case "transaction_timeout":
                fieldName = "transaction_timeout_ms";
                break;
            case "max_lifetime":
                fieldName = "max_lifetime_ms";
                break;
            case "session_timeout":
                fieldName = "session_timeout_ms";
                break;
            case "rebalance_timeout":
                fieldName = "rebalance_timeout_ms";
                break;
            case "expiry_time_period":
                fieldName = "expiry_time_period_ms";
                break;
            case "renew_period":
                fieldName = "renew_period_ms";
                break;
            case "retention_time":
                fieldName = "retention_time_ms";
                break;
            case "heartbeat_interval":
                fieldName = "heartbeat_interval_ms";
                break;
            case "issue_timestamp":
                fieldName = "issue_timestamp_ms";
                break;
            case "expiry_timestamp":
                fieldName = "expiry_timestamp_ms";
                break;
            case "max_timestamp":
                fieldName = "max_timestamp_ms";
                break;
            case "transaction_start_time":
                fieldName = "transaction_start_time_ms";
                break;
            case "log_append_time":
                fieldName = "log_append_time_ms";
                break;
        }

        fieldName = CaseUtils.toCamelCase(fieldName, true, '_');

        if (!knownFieldNames.contains(fieldName)) {
            switch (fieldName) {
                case "IssueTimestampMs":
                    fieldName = "IssueTimestamp";
                    break;
                case "ExpiryTimestampMs":
                    fieldName = "ExpiryTimestamp";
                    break;
                case "MaxTimestampMs":
                    fieldName = "MaxTimestamp";
                    break;
                case "KraftVersionFeature":
                    fieldName = "KRaftVersionFeature";
                    break;
                case "IsKraftController":
                    fieldName = "IsKRaftController";
                    break;
                case "KraftVersion":
                    fieldName = "KRaftVersion";
                    break;
            }
        }

        return fieldName;
    }
}
