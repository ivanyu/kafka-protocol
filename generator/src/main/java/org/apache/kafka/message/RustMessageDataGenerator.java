package org.apache.kafka.message;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RustMessageDataGenerator {
    private final MessageSpec message;
    private final short version;
    private final StructRegistry structRegistry = new StructRegistry();
    private final RustHeaderGenerator headerGenerator = new RustHeaderGenerator();
    private final CodeBuffer buffer = new CodeBuffer();

    public RustMessageDataGenerator(MessageSpec message, short version) {
        if (!message.validVersions().contains(version)) {
            throw new RuntimeException("Unsupported version: " + version);
        }
        if (message.struct().versions().contains(Short.MAX_VALUE)) {
            throw new RuntimeException("Message " + message.name() + " does " +
                    "not specify a maximum version.");
        }
        this.message = message;
        this.version = version;
    }

    public void generateAndWrite(BufferedWriter writer) throws Exception {
        generate();
        write(writer);
    }

    private void generate() throws Exception {
        structRegistry.register(message);

        String className = message.dataClassName();
        if (className.endsWith("Data")) {
            className = className.substring(0, className.length() - 4);
        }

        generateClass(true, className, message.struct());

        headerGenerator.generate();
    }

    private void generateClass(boolean isTopLevel,
                               String className,
                               StructSpec struct) throws Exception {
        headerGenerator.addImport("serde::Serialize");
        headerGenerator.addImport("serde::Deserialize");
        headerGenerator.addImportTest("proptest_derive::Arbitrary");

        buffer.printf("#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]%n");
        buffer.printf("#[cfg_attr(test, derive(Arbitrary))]%n");
        buffer.printf("pub struct %s {%n", className);
        buffer.incrementIndent();
        generateFieldDeclarations(struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        generateClassDefault(className, struct);
        buffer.printf("%n");
        generateClassReader(className, struct);
        buffer.printf("%n");
        generateClassWriter(className, struct);
        buffer.printf("%n");

        generateSubclasses(struct);

        if (isTopLevel) {
            for (Iterator<StructSpec> iter = structRegistry.commonStructs(); iter.hasNext(); ) {
                StructSpec commonStruct = iter.next();
                generateClass(false, commonStruct.name(), commonStruct);
            }

            buffer.printf("#[cfg(test)]%n");
            buffer.printf("mod tests {%n");
            buffer.incrementIndent();
            buffer.printf("use super::*;%n");
            buffer.printf("use proptest::prelude::*;%n");
            buffer.printf("%n");

            buffer.printf("#[test]%n");
            buffer.printf("fn test_java_default() {%n");
            buffer.incrementIndent();
            buffer.printf("crate::test_utils::test_java_default::<%s>(\"%s\", %d);%n", className, className, version);
            buffer.decrementIndent();
            buffer.printf("}%n");

            buffer.printf("%n");

            buffer.printf("proptest! {%n");
            buffer.incrementIndent();
            buffer.printf("#[test]%n");
            buffer.printf("fn test_serde(data: %s) {%n", className);
            buffer.incrementIndent();
            buffer.printf("crate::test_utils::test_serde(&data)?;%n");
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.decrementIndent();
            buffer.printf("}%n");

            buffer.printf("%n");

            buffer.printf("proptest! {%n");
            buffer.incrementIndent();
            buffer.printf("#[test]%n");
            buffer.printf("fn test_java_arbitrary(data: %s) {%n", className);
            buffer.incrementIndent();
            buffer.printf("crate::test_utils::test_java_arbitrary(&data, \"%s\", %d);%n", className, version);
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.decrementIndent();
            buffer.printf("}%n");

            buffer.decrementIndent();

            buffer.printf("}%n");
        }
    }

    private void generateFieldDeclarations(StructSpec struct) {
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);
            String type = RustFieldSpecAdaptor.rustType(field.type(), headerGenerator);
            if (field.nullableVersions().contains(version)) {
                type = "Option<" + type + ">";
            }
            if (type.equals("String")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::string()\"))]%n");
            } else if (type.equals("Option<String>")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::optional_string()\"))]%n");
            } else if (type.equals("Vec<u8>")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                headerGenerator.addImportTest("crate::test_utils::serde_bytes");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::bytes()\"))]%n");
                buffer.printf("#[cfg_attr(test, serde(with=\"serde_bytes\"))]%n");
            } else if (type.equals("Option<Vec<u8>>")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                headerGenerator.addImportTest("crate::test_utils::serde_option_bytes");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::optional_bytes()\"))]%n");
                buffer.printf("#[cfg_attr(test, serde(with=\"serde_option_bytes\"))]%n");
            } else if (type.equals("Uuid")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::uuid()\"))]%n");
            } else if (type.equals("Vec<Uuid>")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::vec_elem::<Uuid>(proptest_strategies::uuid())\"))]%n");
            } else if (type.startsWith("Vec<")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::vec()\"))]%n");
            } else if (type.startsWith("Option<Vec<")) {
                headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
                buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::optional_vec()\"))]%n");
            }
            buffer.printf("pub %s: %s,%n", rustFieldSpecAdaptor.fieldName(), type);
        }

        if (hasTaggedFields()) {
            headerGenerator.addImport("crate::tagged_fields::RawTaggedField");
            headerGenerator.addImportTest("crate::test_utils::proptest_strategies");
            buffer.printf("#[cfg_attr(test, proptest(strategy = \"proptest_strategies::unknown_tagged_fields_empty()\"))]%n");
            buffer.printf("pub _unknown_tagged_fields: Vec<RawTaggedField>,%n");
        }
    }

    private void generateClassDefault(String className, StructSpec struct) {
        buffer.printf("impl Default for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn default() -> Self {%n");
        buffer.incrementIndent();
        buffer.printf("%s {%n", className);
        buffer.incrementIndent();

        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);
            String fieldNameInRust = rustFieldSpecAdaptor.fieldName();
            buffer.printf("%s: %s,%n", fieldNameInRust, rustFieldSpecAdaptor.fieldDefault());
        }
        if (hasTaggedFields()) {
            buffer.printf("_unknown_tagged_fields: Vec::new(),%n");
        }

        buffer.decrementIndent();
        buffer.printf("}%n", className);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassReader(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Read");
        headerGenerator.addImport("std::io::Result");
        headerGenerator.addImport("crate::readable_writable::KafkaReadable");
        buffer.printf("impl KafkaReadable for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {%n");
        buffer.incrementIndent();

        List<String> fieldsForConstructor = new ArrayList<>();

        Map<Integer, FieldSpec> taggedFields = new HashMap<>();
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);
            String fieldNameInRust = rustFieldSpecAdaptor.fieldName();
            fieldsForConstructor.add(fieldNameInRust);

            if (field.taggedVersions().contains(version)) {
                taggedFields.put(field.tag().get(), field);
                buffer.printf("let mut %s = %s;%n", fieldNameInRust, rustFieldSpecAdaptor.fieldDefault());
            } else {
                String readExpression = readExpression(
                        "input",
                        field.type(),
                        fieldFlexibleVersions(field).contains(version),
                        field.nullableVersions().contains(version),
                        rustFieldSpecAdaptor.fieldName()
                );
                buffer.printf("let %s = %s?;%n", rustFieldSpecAdaptor.fieldName(), readExpression);
            }
        }

        if (hasTaggedFields()) {
            if (taggedFields.isEmpty()) {
                buffer.printf("let tagged_fields_callback = |tag: i32, _: &[u8]| {%n");
            } else {
                buffer.printf("let tagged_fields_callback = |tag: i32, tag_data: &[u8]| {%n");
            }
            buffer.incrementIndent();

            buffer.printf("match tag {%n");
            buffer.incrementIndent();

            taggedFields.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).forEach(kv -> {
                buffer.printf("%d => {%n", kv.getKey());
                buffer.incrementIndent();
                headerGenerator.addImport("std::io::Cursor");
                buffer.printf("let mut cur = Cursor::new(tag_data);%n");
                RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(kv.getValue(), version, headerGenerator);
                String readExpression = readExpression(
                    "&mut cur",
                    kv.getValue().type(),
                    fieldFlexibleVersions(kv.getValue()).contains(version),
                    kv.getValue().nullableVersions().contains(version),
                    rustFieldSpecAdaptor.fieldName()
                );
                buffer.printf("%s = %s?;%n", rustFieldSpecAdaptor.fieldName(), readExpression);
                buffer.printf("Ok(true)%n");
                buffer.decrementIndent();
                buffer.printf("},%n");
            });

            buffer.printf("_ => Ok(false)%n");

            buffer.decrementIndent();
            buffer.printf("}%n");

            buffer.decrementIndent();
            buffer.printf("};%n");
            headerGenerator.addImport("crate::tagged_fields::k_read_tagged_fields");
            buffer.printf("let _unknown_tagged_fields = k_read_tagged_fields(input, tagged_fields_callback)?;%n");

            fieldsForConstructor.add("_unknown_tagged_fields");
        }

        buffer.printf("Ok(%s {%n", className);
        buffer.incrementIndent();
        buffer.printf("%s%n", String.join(", ", fieldsForConstructor));
        buffer.decrementIndent();
        buffer.printf("})%n");

        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String arrayReadExpression(String readSource, FieldType type, boolean flexible, boolean nullable, String fieldNameInRust) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
        String rustElementType = RustFieldSpecAdaptor.rustType(arrayType.elementType(), headerGenerator);

        if (arrayType.elementType().isString()) {
            if (nullable) {
                headerGenerator.addImport("crate::str_arrays::k_read_nullable_array_of_strings");
                return String.format("k_read_nullable_array_of_strings(%s, \"%s\", %b)",
                    readSource, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::str_arrays::k_read_array_of_strings");
                return String.format("k_read_array_of_strings(%s, \"%s\", %b)",
                    readSource, fieldNameInRust, flexible);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::arrays::k_read_nullable_array");
                return String.format("k_read_nullable_array::<%s>(%s, \"%s\", %b)",
                    rustElementType, readSource, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::arrays::k_read_array");
                return String.format("k_read_array::<%s>(%s, \"%s\", %b)",
                    rustElementType, readSource, fieldNameInRust, flexible);
            }
        }
    }

    private String readExpression(String readSource, FieldType type, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (type.isString()) {
            return stringReadExpression(readSource, flexible, nullable, fieldNameInRust);
        } else if (type.isBytes()) {
            return byteReadExpression(readSource, flexible, nullable, fieldNameInRust);
        } else if (type.isArray()) {
            return arrayReadExpression(readSource, type, flexible, nullable, fieldNameInRust);
        } else {
            String readExpression = primitiveReadExpression(readSource, type);
            if (nullable) {
                headerGenerator.addImport("crate::readable_writable::KafkaReadable");
                return String.format("(if i8::read(input)? < 0 { Ok(None) } else { %s.map(Some) })", readExpression);
            } else {
                return readExpression;
            }
        }
    }

    private String primitiveReadExpression(String readSource, FieldType type) {
        if (type instanceof FieldType.RecordsFieldType) {
            throw new RuntimeException("not supported yet");
        } else if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("bool::read(%s)", readSource);
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("i8::read(%s)", readSource);
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("i16::read(%s)", readSource);
        } else if (type instanceof FieldType.Uint16FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("u16::read(%s)", readSource);
        } else if (type instanceof FieldType.Uint32FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("u32::read(%s)", readSource);
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("i32::read(%s)", readSource);
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("i64::read(%s)", readSource);
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            headerGenerator.addImport("uuid::Uuid");
            return String.format("Uuid::read(%s)", readSource);
        } else if (type instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport("crate::readable_writable::KafkaReadable");
            return String.format("f64::read(%s)", readSource);
        } else if (type.isStruct()) {
            return String.format("%s::read(%s)", type, readSource);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String stringReadExpression(String readSource, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::strings::k_read_nullable_string");
            return String.format("k_read_nullable_string(%s, \"%s\", %b)", readSource, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::strings::k_read_string");
            return String.format("k_read_string(%s, \"%s\", %b)", readSource, fieldNameInRust, flexible);
        }
    }

    private String byteReadExpression(String readSource, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::bytes::k_read_nullable_bytes");
            return String.format("k_read_nullable_bytes(%s, \"%s\", %b)", readSource, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::bytes::k_read_bytes");
            return String.format("k_read_bytes(%s, \"%s\", %b)", readSource, fieldNameInRust, flexible);
        }
    }

    private void generateClassWriter(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Write");
        headerGenerator.addImport("std::io::Result");
        headerGenerator.addImport("crate::readable_writable::KafkaWritable");
        buffer.printf("impl KafkaWritable for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn write(&self, #[allow(unused)] output: &mut impl Write) -> Result<()> {%n");
        buffer.incrementIndent();

        Map<Integer, FieldSpec> taggedFields = new HashMap<>();
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);

            if (field.taggedVersions().contains(version)) {
                taggedFields.put(field.tag().get(), field);
                continue;
            }

            boolean flexible = fieldFlexibleVersions(field).contains(version);
            boolean nullable = field.nullableVersions().contains(version);
            String writeExpression = writeExpression("output", field.type(), flexible, nullable, rustFieldSpecAdaptor.fieldName());
            buffer.printf("%s?;%n", writeExpression);
        }
        if (hasTaggedFields()) {
            if (!taggedFields.isEmpty()) {
                buffer.printf("let mut known_tagged_fields = Vec::<RawTaggedField>::new();%n");
                taggedFields.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).forEach(kv -> {
                    FieldSpec field = kv.getValue();
                    RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);
                    rustFieldSpecAdaptor.generateNonDefaultValueCheck(buffer, "self.");
                    buffer.incrementIndent();
                    buffer.printf("let mut cur = Cursor::new(Vec::<u8>::new());%n");
                    boolean flexible = fieldFlexibleVersions(field).contains(version);
                    boolean nullable = field.nullableVersions().contains(version);
                    String writeExpression = writeExpression("&mut cur", field.type(), flexible, nullable, rustFieldSpecAdaptor.fieldName());
                    buffer.printf("%s?;%n", writeExpression);
                    buffer.printf("known_tagged_fields.push(RawTaggedField { tag: %d, data: cur.into_inner() });%n", field.tag().get());
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                });
                headerGenerator.addImport("crate::tagged_fields::k_write_tagged_fields");
                buffer.printf("k_write_tagged_fields(output, &known_tagged_fields, &self._unknown_tagged_fields)?;%n");
            } else {
                headerGenerator.addImport("crate::tagged_fields::k_write_tagged_fields");
                buffer.printf("k_write_tagged_fields(output, &[], &self._unknown_tagged_fields)?;%n");
            }
        }

        buffer.printf("Ok(())%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String writeExpression(String writeTarget,
                                   FieldType type,
                                   boolean flexible,
                                   boolean nullable,
                                   String fieldNameInRust) {
        if (type.isString()) {
            return stringWriteExpression(writeTarget, flexible, nullable, fieldNameInRust);
        } else if (type.isBytes()) {
            return bytesWriteExpression(writeTarget, flexible, nullable, fieldNameInRust);
        } else if (type.isArray()) {
            return arrayWriteExpression(writeTarget, type, flexible, nullable, fieldNameInRust);
        } else {
            if (nullable) {
                String nonNullVar = type.isRecords() ? "_" : "v";
                String result = String.format("(if let Some(%s) = &self.%s {", nonNullVar, fieldNameInRust);
                result += String.format(" 1_i8.write(%s)?; ", writeTarget);
                String writeExpression = primitiveWriteExpression(writeTarget, type, nonNullVar);
                result += String.format("%s", writeExpression);
                result += " } else { ";
                result += String.format("(-1_i8).write(%s)", writeTarget);
                result += " })";
                return result;
            } else {
                return primitiveWriteExpression(writeTarget, type, String.format("self.%s", fieldNameInRust));
            }
        }
    }

    private String stringWriteExpression(String writeTarget, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::strings::k_write_nullable_string");
            return String.format("k_write_nullable_string(%s, \"%s\", self.%s.as_deref(), %b)",
                writeTarget, fieldNameInRust, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::strings::k_write_string");
            return String.format("k_write_string(%s, \"%s\", &self.%s, %b)",
                writeTarget, fieldNameInRust, fieldNameInRust, flexible);
        }
    }

    private String bytesWriteExpression(String writeTarget, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::bytes::k_write_nullable_bytes");
            return String.format("k_write_nullable_bytes(%s, \"%s\", self.%s.as_deref(), %b)",
                writeTarget, fieldNameInRust, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::bytes::k_write_bytes");
            return String.format("k_write_bytes(%s, \"%s\", &self.%s, %b)",
                writeTarget, fieldNameInRust, fieldNameInRust, flexible);
        }
    }

    private String arrayWriteExpression(String writeTarget,
                                        FieldType type,
                                        boolean flexible,
                                        boolean nullable,
                                        String fieldNameInRust) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) type;

        if (arrayType.elementType().isString()) {
            if (nullable) {
                headerGenerator.addImport("crate::str_arrays::k_write_nullable_array_of_strings");
                return String.format("k_write_nullable_array_of_strings(%s, \"%s\", self.%s.as_deref(), %b)",
                    writeTarget, fieldNameInRust, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::str_arrays::k_write_array_of_strings");
                return String.format("k_write_array_of_strings(%s, \"%s\", &self.%s, %b)",
                    writeTarget, fieldNameInRust, fieldNameInRust, flexible);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::arrays::k_write_nullable_array");
                return String.format("k_write_nullable_array(%s, \"%s\", self.%s.as_deref(), %b)",
                    writeTarget, fieldNameInRust, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::arrays::k_write_array");
                return String.format("k_write_array(%s, \"%s\", &self.%s, %b)",
                    writeTarget, fieldNameInRust, fieldNameInRust, flexible);
            }
        }
    }

    private String primitiveWriteExpression(String writeTarget, FieldType type, String object) {
        if (type instanceof FieldType.RecordsFieldType) {
            headerGenerator.addImport("std::io::Error");
            return "Ok::<(), Error>(())";
        } else if (type instanceof FieldType.BoolFieldType
            || type instanceof FieldType.Int8FieldType
            || type instanceof FieldType.Int16FieldType
            || type instanceof FieldType.Uint16FieldType
            || type instanceof FieldType.Uint32FieldType
            || type instanceof FieldType.Int32FieldType
            || type instanceof FieldType.Int64FieldType
            || type instanceof FieldType.UUIDFieldType
            || type instanceof FieldType.Float64FieldType
            || type.isStruct()
        ) {
            headerGenerator.addImport("crate::readable_writable::KafkaWritable");
            return String.format("%s.write(%s)", object, writeTarget);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private Versions fieldFlexibleVersions(FieldSpec field) {
        if (field.flexibleVersions().isPresent()) {
            if (!message.flexibleVersions().intersect(field.flexibleVersions().get()).
                    equals(field.flexibleVersions().get())) {
                throw new RuntimeException("The flexible versions for field " +
                        field.name() + " are " + field.flexibleVersions().get() +
                        ", which are not a subset of the flexible versions for the " +
                        "message as a whole, which are " + message.flexibleVersions());
            }
            return field.flexibleVersions().get();
        } else {
            return message.flexibleVersions();
        }
    }

    private void generateSubclasses(StructSpec struct) throws Exception {
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                if (!structRegistry.commonStructNames().contains(arrayType.elementName())) {
                    generateClass(false, arrayType.elementType().toString(),
                            structRegistry.findStruct(field));
                }
            } else if (field.type().isStruct()) {
                if (!structRegistry.commonStructNames().contains(field.typeString())) {
                    generateClass(false, field.typeString(), structRegistry.findStruct(field));
                }
            }
        }
    }

    private boolean hasTaggedFields() {
        return message.flexibleVersions().contains(this.version);
    }

    void write(BufferedWriter writer) throws Exception {
        headerGenerator.buffer.write(writer);
        buffer.write(writer);
    }
}
