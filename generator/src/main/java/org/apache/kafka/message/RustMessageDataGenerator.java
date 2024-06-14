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

        generateClass(true, message.dataClassName(), message.struct());

        headerGenerator.generate();
    }

    private void generateClass(boolean isTopLevel,
                               String className,
                               StructSpec struct) throws Exception {
        headerGenerator.addImport("serde::Serialize");
        headerGenerator.addImport("serde::Deserialize");

        buffer.printf("#[derive(Serialize, Deserialize)]%n");
        buffer.printf("pub struct %s {%n", className);
        buffer.incrementIndent();
        generateFieldDeclarations(struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        buffer.printf("impl %s {%n", className);
        buffer.incrementIndent();
        generateClassReader(className, struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
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
            buffer.printf("%n");

            buffer.printf("#[test]%n");
            buffer.printf("fn it_works() {%n");
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

            String type = rustType(field.type(), headerGenerator);
            if (field.nullableVersions().contains(version)) {
                type = "Option<" + type + ">";
            }
            buffer.printf("pub %s: %s,%n", fieldName(field), type);
        }
    }

    private void generateClassReader(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Read");
        headerGenerator.addImport("std::io::Result");
        buffer.printf("pub fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {%n");
        buffer.incrementIndent();

        List<String> fieldsForConstructor = new ArrayList<>();

        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            final String fieldNameInRust = fieldName(field);
            fieldsForConstructor.add(fieldNameInRust);
            if (field.type().isString()) {
                generateReadForString(field);
            } else if (field.type().isBytes()) {
                generateReadForBytes(field);
            } else if (field.type().isArray()) {
                generateReadForArray(field);
            } else {
                final String readExpression = primitiveReadExpression(field.type());
                if (field.nullableVersions().contains(version)) {
                    buffer.printf("let %s = if input.read_i8()? < 0 {%n", fieldNameInRust);
                    buffer.incrementIndent();
                    buffer.printf("None%n");
                    buffer.decrementIndent();
                    buffer.printf("} else {%n");
                    buffer.incrementIndent();
                    buffer.printf("Some(%s)%n", readExpression);
                    buffer.decrementIndent();
                    buffer.printf("};%n");
                } else {
                    buffer.printf("let %s = %s;%n", fieldNameInRust, readExpression);
                }
            }
        }

        buffer.printf("Ok(%s {%n", className);
        buffer.incrementIndent();
        buffer.printf("%s%n", String.join(", ", fieldsForConstructor));
        buffer.decrementIndent();
        buffer.printf("})%n");

        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateReadForString(FieldSpec field) {
        buffer.printf("let %s = %s;%n", fieldName(field),
                stringReadExpression(field, field.nullableVersions().contains(version), fieldName(field))
        );
    }

    private void generateReadForBytes(FieldSpec field) {
        buffer.printf("let %s = {%n", fieldName(field));
        buffer.incrementIndent();
        if (fieldFlexibleVersions(field).contains(version)) {
            headerGenerator.addImport("varint_rs::VarintReader");
            buffer.printf("let bytes_len = (input.read_u32_varint()? as i32) - 1;%n");
        } else {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            buffer.printf("let bytes_len = input.read_i32::<BigEndian>()?;%n");
        }
        buffer.printf("if bytes_len < 0 {%n");
        buffer.incrementIndent();
        if (field.nullableVersions().contains(version)) {
            buffer.printf("None%n");
        } else {
            headerGenerator.addImport("std::io::Error");
            headerGenerator.addImport("std::io::ErrorKind");
            buffer.printf("// TODO replace with proper error%n");
            buffer.printf("return Err(Error::new(ErrorKind::Other, \"non-nullable field %s was serialized as null\"));%n",
                    fieldName(field));
        }
        buffer.decrementIndent();
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        buffer.printf("let mut buf = vec![0_u8; bytes_len as usize];%n");
        buffer.printf("input.read_exact(&mut buf)?;%n");
        if (field.nullableVersions().contains(version)) {
            buffer.printf("Some(buf)%n");
        } else {
            buffer.printf("buf%n");
        }
        buffer.decrementIndent();
        buffer.printf("}%n");

        buffer.decrementIndent();
        buffer.printf("};%n");
    }

    private void generateReadForArray(FieldSpec field) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();

        buffer.printf("let %s = {%n", fieldName(field));
        buffer.incrementIndent();
        if (fieldFlexibleVersions(field).contains(version)) {
            headerGenerator.addImport("varint_rs::VarintReader");
            buffer.printf("let arr_len = (input.read_u32_varint()? as i32) - 1;%n");
        } else {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            buffer.printf("let arr_len = input.read_i32::<BigEndian>()?;%n");
        }
        buffer.printf("if arr_len < 0 {%n");
        buffer.incrementIndent();
        if (field.nullableVersions().contains(version)) {
            buffer.printf("None%n");
        } else {
            headerGenerator.addImport("std::io::Error");
            headerGenerator.addImport("std::io::ErrorKind");
            buffer.printf("// TODO replace with proper error%n");
            buffer.printf("return Err(Error::new(ErrorKind::Other, \"non-nullable field %s was serialized as null\"));%n",
                    fieldName(field));
        }
        buffer.decrementIndent();
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        if (arrayType.elementType().isArray()) {
            throw new RuntimeException("Nested arrays are not supported.  " +
                    "Use an array of structures containing another array.");
        } else {
            buffer.printf("let mut vec: Vec<%s> = Vec::with_capacity(arr_len as usize);%n", rustType(arrayType.elementType(), headerGenerator));
            buffer.printf("for _ in 0..arr_len {%n");
            buffer.incrementIndent();

            if (arrayType.elementType().isBytes()) {
                buffer.printf("TODO%n");
            } else if (arrayType.elementType().isString()) {
                buffer.printf("vec.push(%s);%n", stringReadExpression(field, false, fieldName(field)));
            } else {
                buffer.printf("vec.push(%s);%n", primitiveReadExpression(arrayType.elementType()));
            }

            buffer.decrementIndent();
            buffer.printf("}%n");
            if (field.nullableVersions().contains(version)) {
                buffer.printf("Some(vec)%n");
            } else {
                buffer.printf("vec%n");
            }
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("};%n");
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.RecordsFieldType) {
            return "BaseRecords {}";
        } else if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_i8()? != 0";
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_i8()?";
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_i16::<BigEndian>()?";
        } else if (type instanceof FieldType.Uint16FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_u16::<BigEndian>()?";
        } else if (type instanceof FieldType.Uint32FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_u32::<BigEndian>()?";
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_i32::<BigEndian>()?";
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_i64::<BigEndian>()?";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "Uuid::from_u128(input.read_u128::<BigEndian>()?)";
        } else if (type instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport("byteorder::BigEndian");
            headerGenerator.addImport("byteorder::ReadBytesExt");
            return "input.read_f64::<BigEndian>()?";
        } else if (type.isStruct()) {
            return String.format("%s::read(input)?", type);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String stringReadExpression(FieldSpec field, boolean nullable, String fieldNameInRust) {
        if (fieldFlexibleVersions(field).contains(version)) {
            if (nullable) {
                headerGenerator.addImport("crate::string::read_nullable_compact_string");
                return String.format("read_nullable_compact_string(input, \"%s\")?", fieldNameInRust);
            } else {
                headerGenerator.addImport("crate::string::read_compact_string");
                return String.format("read_compact_string(input, \"%s\")?", fieldNameInRust);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::string::read_nullable_string");
                return String.format("read_nullable_string(input, \"%s\")?", fieldNameInRust);
            } else {
                headerGenerator.addImport("crate::string::read_string");
                return String.format("read_string(input, \"%s\")?", fieldNameInRust);
            }
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

    void write(BufferedWriter writer) throws Exception {
        headerGenerator.buffer.write(writer);
        buffer.write(writer);
    }

    private String fieldName(FieldSpec field) {
        final String snakeCaseName = field.snakeCaseName();
        if (snakeCaseName.equals("type")) {
            return "type_";
        } else if (snakeCaseName.equals("match")) {
            return "match_";
        } else {
            return snakeCaseName;
        }
    }

    private String rustType(FieldType type, RustHeaderGenerator headerGenerator) {
        if (type instanceof FieldType.BoolFieldType) {
            return "bool";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "i8";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "i16";
        } else if (type instanceof FieldType.Uint16FieldType) {
            return "u16";
        } else if (type instanceof FieldType.Uint32FieldType) {
            return "u32";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "i32";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "i64";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport("uuid::Uuid");
            return "Uuid";
        } else if (type instanceof FieldType.Float64FieldType) {
            return "f64";
        } else if (type.isString()) {
            return "String";
        } else if (type.isBytes()) {
            return "Vec<u8>";
        } else if (type instanceof FieldType.RecordsFieldType) {
            headerGenerator.addImport("crate::types::BaseRecords");
            return "BaseRecords";
        } else if (type.isStruct()) {
            return MessageGenerator.capitalizeFirst(type.toString());
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            return String.format("Vec<%s>", rustType(arrayType.elementType(), headerGenerator));
        } else {
            throw new RuntimeException("Unknown field type " + type);
        }
    }
}
