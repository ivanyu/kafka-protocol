package org.apache.kafka.message;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
            buffer.printf("pub %s: %s,%n", fieldName(field), type);
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

            String fieldNameInRust = fieldName(field);
            RustFieldSpecAdaptor rustFieldSpecAdaptor = new RustFieldSpecAdaptor(field, version, headerGenerator);
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
        headerGenerator.addImport("crate::primitives::KafkaReadable");
        buffer.printf("impl KafkaReadable for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {%n");
        buffer.incrementIndent();

        List<String> fieldsForConstructor = new ArrayList<>();

        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            String fieldNameInRust = fieldName(field);
            fieldsForConstructor.add(fieldNameInRust);
            String readExpression = readExpression(
                field.type(),
                fieldFlexibleVersions(field).contains(version),
                field.nullableVersions().contains(version),
                fieldName(field)
            );
            buffer.printf("let %s = %s?;%n", fieldName(field), readExpression);
        }

        if (hasTaggedFields()) {
            headerGenerator.addImport("crate::tagged_fields::k_read_unknown_tagged_fields");
            buffer.printf("let _unknown_tagged_fields = k_read_unknown_tagged_fields(input)?;%n");
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

    private String arrayReadExpression(FieldType type, boolean flexible, boolean nullable, String fieldNameInRust) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
        String rustElementType = RustFieldSpecAdaptor.rustType(arrayType.elementType(), headerGenerator);

        if (arrayType.elementType().isString()) {
            if (nullable) {
                headerGenerator.addImport("crate::str_arrays::k_read_nullable_array_of_strings");
                return String.format("k_read_nullable_array_of_strings(input, \"%s\", %b)",
                    fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::str_arrays::k_read_array_of_strings");
                return String.format("k_read_array_of_strings(input, \"%s\", %b)",
                    fieldNameInRust, flexible);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::arrays::k_read_nullable_array");
                return String.format("k_read_nullable_array::<%s>(input, \"%s\", %b)",
                    rustElementType, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::arrays::k_read_array");
                return String.format("k_read_array::<%s>(input, \"%s\", %b)",
                    rustElementType, fieldNameInRust, flexible);
            }
        }
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.RecordsFieldType) {
            throw new RuntimeException("not supported yet");
        } else if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "bool::read(input)";
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "i8::read(input)";
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "i16::read(input)";
        } else if (type instanceof FieldType.Uint16FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "u16::read(input)";
        } else if (type instanceof FieldType.Uint32FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "u32::read(input)";
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "i32::read(input)";
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "i64::read(input)";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            headerGenerator.addImport("uuid::Uuid");
            return "Uuid::read(input)";
        } else if (type instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport("crate::primitives::KafkaReadable");
            return "f64::read(input)";
        } else if (type.isStruct()) {
            return String.format("%s::read(input)", type);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String readExpression(FieldType type, boolean flexible, boolean nullable, String fieldNameInRust) {
        if (type.isString()) {
            return stringReadExpression(flexible, nullable, fieldNameInRust);
        } else if (type.isBytes()) {
            return byteReadExpression(flexible, nullable, fieldNameInRust);
        } else if (type.isArray()) {
            return arrayReadExpression(type, flexible, nullable, fieldNameInRust);
        } else {
            String readExpression = primitiveReadExpression(type);
            if (nullable) {
                headerGenerator.addImport("crate::primitives::KafkaReadable");
                return String.format("(if i8::read(input)? < 0 { Ok(None) } else { %s.map(Some) })", readExpression);
            } else {
                return readExpression;
            }
        }
    }

    private String stringReadExpression(boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::strings::k_read_nullable_string");
            return String.format("k_read_nullable_string(input, \"%s\", %b)", fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::strings::k_read_string");
            return String.format("k_read_string(input, \"%s\", %b)", fieldNameInRust, flexible);
        }
    }

    private String byteReadExpression(boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::bytes::k_read_nullable_bytes");
            return String.format("k_read_nullable_bytes(input, \"%s\", %b)", fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::bytes::k_read_bytes");
            return String.format("k_read_bytes(input, \"%s\", %b)", fieldNameInRust, flexible);
        }
    }

    private void generateClassWriter(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Write");
        headerGenerator.addImport("std::io::Result");
        headerGenerator.addImport("crate::primitives::KafkaWritable");
        buffer.printf("impl KafkaWritable for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn write(&self, #[allow(unused)] output: &mut impl Write) -> Result<()> {%n");
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            boolean flexible = fieldFlexibleVersions(field).contains(version);
            boolean nullable = field.nullableVersions().contains(version);
            if (field.type().isString()) {
                buffer.printf("%s?;%n",
                    stringWriteExpression(flexible, nullable, fieldName(field))
                );
            } else if (field.type().isBytes()) {
                buffer.printf("%s?;%n",
                    bytesWriteExpression(flexible, nullable, fieldName(field))
                );
            } else if (field.type().isArray()) {
                buffer.printf("%s?;%n",
                    arrayWriteExpression(field.type(), flexible, nullable, fieldName(field))
                );
            } else {
                if (nullable) {
                    String nonNullVar = field.type().isRecords() ? "_" : "v";
                    buffer.printf("if let Some(%s) = &self.%s {%n", nonNullVar, fieldName(field));
                    buffer.incrementIndent();
                    buffer.printf("1_i8.write(output)?;%n");
                    String writeExpression = primitiveWriteExpression(field.type(), nonNullVar);
                    buffer.printf("%s?;%n", writeExpression);
                    buffer.decrementIndent();
                    buffer.printf("} else {%n");
                    buffer.incrementIndent();
                    buffer.printf("(-1_i8).write(output)?;%n");
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                } else {
                    String writeExpression = primitiveWriteExpression(
                        field.type(),
                        String.format("self.%s", fieldName(field))
                    );
                    buffer.printf("%s?;%n", writeExpression);
                }
            }
        }
        if (hasTaggedFields()) {
            headerGenerator.addImport("crate::tagged_fields::k_write_unknown_tagged_fields");
            buffer.printf("k_write_unknown_tagged_fields(output, &self._unknown_tagged_fields)?;%n");
        }

        buffer.printf("Ok(())%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String stringWriteExpression(boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::strings::k_write_nullable_string");
            return String.format("k_write_nullable_string(output, \"%s\", self.%s.as_deref(), %b)",
                fieldNameInRust, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::strings::k_write_string");
            return String.format("k_write_string(output, \"%s\", &self.%s, %b)",
                fieldNameInRust, fieldNameInRust, flexible);
        }
    }

    private String bytesWriteExpression(boolean flexible, boolean nullable, String fieldNameInRust) {
        if (nullable) {
            headerGenerator.addImport("crate::bytes::k_write_nullable_bytes");
            return String.format("k_write_nullable_bytes(output, \"%s\", self.%s.as_deref(), %b)",
                fieldNameInRust, fieldNameInRust, flexible);
        } else {
            headerGenerator.addImport("crate::bytes::k_write_bytes");
            return String.format("k_write_bytes(output, \"%s\", &self.%s, %b)",
                fieldNameInRust, fieldNameInRust, flexible);
        }
    }

    private String arrayWriteExpression(FieldType type, boolean flexible,
                                        boolean nullable,
                                        String fieldNameInRust) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) type;

        if (arrayType.elementType().isString()) {
            if (nullable) {
                headerGenerator.addImport("crate::str_arrays::k_write_nullable_array_of_strings");
                return String.format("k_write_nullable_array_of_strings(output, \"%s\", self.%s.as_deref(), %b)",
                    fieldNameInRust, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::str_arrays::k_write_array_of_strings");
                return String.format("k_write_array_of_strings(output, \"%s\", &self.%s, %b)",
                    fieldNameInRust, fieldNameInRust, flexible);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::arrays::k_write_nullable_array");
                return String.format("k_write_nullable_array(output, \"%s\", self.%s.as_deref(), %b)",
                    fieldNameInRust, fieldNameInRust, flexible);
            } else {
                headerGenerator.addImport("crate::arrays::k_write_array");
                return String.format("k_write_array(output, \"%s\", &self.%s, %b)",
                    fieldNameInRust, fieldNameInRust, flexible);
            }
        }
    }

    private String primitiveWriteExpression(FieldType type, String object) {
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
            headerGenerator.addImport("crate::primitives::KafkaWritable");
            return String.format("%s.write(output)", object);
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

    private String fieldName(FieldSpec field) {
        String snakeCaseName = field.snakeCaseName();
        if (snakeCaseName.equals("type")) {
            return "type_";
        } else if (snakeCaseName.equals("match")) {
            return "match_";
        } else {
            return snakeCaseName;
        }
    }
}
