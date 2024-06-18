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

        generateClassReader(className, struct);
        buffer.printf("%n");

        buffer.printf("impl %s {%n", className);
        buffer.incrementIndent();
        generateClassWriter(className, struct);
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
            if (field.nullableVersions().contains(version) || field.type().isBytes()) {
                type = "Option<" + type + ">";
            }
            buffer.printf("pub %s: %s,%n", fieldName(field), type);
        }
    }

    private void generateClassReader(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Read");
        headerGenerator.addImport("std::io::Result");
        headerGenerator.addImport("crate::types::ReadableStruct");
        buffer.printf("impl ReadableStruct for %s {%n", className);
        buffer.incrementIndent();
        buffer.printf("fn read(#[allow(unused)] input: &mut impl Read) -> Result<Self> {%n");
        buffer.incrementIndent();

        List<String> fieldsForConstructor = new ArrayList<>();

        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            final String fieldNameInRust = fieldName(field);
            fieldsForConstructor.add(fieldNameInRust);
            final String readExpression = readExpression(
                field.type(),
                fieldFlexibleVersions(field).contains(version),
                field.nullableVersions().contains(version),
                fieldName(field)
            );
            buffer.printf("let %s = %s?;%n", fieldName(field), readExpression);
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
        final String rustElementType = rustType(arrayType.elementType(), headerGenerator);

        if (arrayType.elementType().isString()) {
            if (flexible) {
                if (nullable) {
                    headerGenerator.addImport("crate::read::k_read_nullable_array_of_strings");
                    return "k_read_nullable_array_of_strings(input)";
                } else {
                    headerGenerator.addImport("crate::read::k_read_array_of_strings");
                    return String.format("k_read_array_of_strings(input, \"%s\")", fieldNameInRust);
                }
            } else {
                if (nullable) {
                    headerGenerator.addImport("crate::read::k_read_nullable_compact_array_of_strings");
                    return "k_read_nullable_compact_array_of_strings(input)";
                } else {
                    headerGenerator.addImport("crate::read::k_read_compact_array_of_strings");
                    return String.format("k_read_compact_array_of_strings(input, \"%s\")", fieldNameInRust);
                }
            }
        } else {
            if (flexible) {
                if (nullable) {
                    headerGenerator.addImport("crate::read::k_read_nullable_array");
                    return String.format("k_read_nullable_array::<%s>(input)", rustElementType);
                } else {
                    headerGenerator.addImport("crate::read::k_read_array");
                    return String.format("k_read_array::<%s>(input, \"%s\")", rustElementType, fieldNameInRust);
                }
            } else {
                if (nullable) {
                    headerGenerator.addImport("crate::read::k_read_nullable_compact_array");
                    return String.format("k_read_nullable_compact_array::<%s>(input)", rustElementType);
                } else {
                    headerGenerator.addImport("crate::read::k_read_compact_array");
                    return String.format("k_read_compact_array::<%s>(input, \"%s\")", rustElementType, fieldNameInRust);
                }
            }
        }
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.RecordsFieldType) {
            headerGenerator.addImport("std::io::Error");
            return "Ok::<BaseRecords, Error>(BaseRecords {})";
        } else if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport("crate::read::k_read_bool");
            return "k_read_bool(input)";
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport("crate::read::k_read_i8");
            return "k_read_i8(input)";
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport("crate::read::k_read_i16");
            return "k_read_i16(input)";
        } else if (type instanceof FieldType.Uint16FieldType) {
            headerGenerator.addImport("crate::read::k_read_u16");
            return "k_read_u16(input)";
        } else if (type instanceof FieldType.Uint32FieldType) {
            headerGenerator.addImport("crate::read::k_read_u32");
            return "k_read_u32(input)";
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport("crate::read::k_read_i32");
            return "k_read_i32(input)";
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport("crate::read::k_read_i64");
            return "k_read_i64(input)";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport("crate::read::k_read_uuid");
            return "k_read_uuid(input)";
        } else if (type instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport("crate::read::k_read_f64");
            return "k_read_f64(input)";
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
            headerGenerator.addImport("crate::read::k_read_bytes");
            return "k_read_bytes(input)";
        } else if (type.isArray()) {
            return arrayReadExpression(type, flexible, nullable, fieldNameInRust);
        } else {
            final String readExpression = primitiveReadExpression(type);
            if (nullable) {
                headerGenerator.addImport("crate::read::k_read_i8");
                return String.format("(if k_read_i8(input)? < 0 { Ok(None) } else { %s.map(Some) })", readExpression);
            } else {
                return readExpression;
            }
        }
    }

    private String stringReadExpression(boolean flexible, boolean nullable, String fieldNameInRust) {
        if (flexible) {
            if (nullable) {
                headerGenerator.addImport("crate::read::k_read_nullable_compact_string");
                return String.format("k_read_nullable_compact_string(input, \"%s\")", fieldNameInRust);
            } else {
                headerGenerator.addImport("crate::read::k_read_compact_string");
                return String.format("k_read_compact_string(input, \"%s\")", fieldNameInRust);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::read::k_read_nullable_string");
                return "k_read_nullable_string(input)";
            } else {
                headerGenerator.addImport("crate::read::k_read_string");
                return String.format("k_read_string(input, \"%s\")", fieldNameInRust);
            }
        }
    }

    private void generateClassWriter(String className, StructSpec struct) {
        headerGenerator.addImport("std::io::Write");
        headerGenerator.addImport("std::io::Result");
        buffer.printf("pub fn write(&self, #[allow(unused)] output: &mut impl Write) -> Result<()> {%n");
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            if (!field.versions().contains(version)) {
                continue;
            }

            if (field.type().isString()) {
                generateWriteForString(field);
            } else if (field.type().isBytes()) {
                generateWriteForBytes(field);
            } else if (field.type().isArray()) {
                generateWriteForArray(field);
            } else {
                buffer.printf("todo!();%n");
            }
        }

        buffer.printf("Ok(())%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateWriteForString(FieldSpec field) {
        buffer.printf("%s;%n",
            stringWriteExpression(field, field.nullableVersions().contains(version), fieldName(field))
        );
    }

    private String stringWriteExpression(FieldSpec field, boolean nullable, String fieldNameInRust) {
        if (fieldFlexibleVersions(field).contains(version)) {
            if (nullable) {
                headerGenerator.addImport("crate::string::write_nullable_compact_string");
                return String.format("write_nullable_compact_string(output, self.%s.as_deref())?", fieldNameInRust);
            } else {
                headerGenerator.addImport("crate::string::write_compact_string");
                return String.format("write_compact_string(output, &self.%s)?", fieldNameInRust);
            }
        } else {
            if (nullable) {
                headerGenerator.addImport("crate::string::write_nullable_string");
                return String.format("write_nullable_string(output, self.%s.as_deref())?", fieldNameInRust);
            } else {
                headerGenerator.addImport("crate::string::write_string");
                return String.format("write_string(output, &self.%s)?", fieldNameInRust);
            }
        }
    }

    private void generateWriteForBytes(FieldSpec field) {
        headerGenerator.addImport("crate::bytes::write_bytes");
        buffer.printf("write_bytes(output, self.%s.as_deref())?;%n", fieldName(field));
    }

    private void generateWriteForArray(FieldSpec field) {
        buffer.printf("todo!();%n");
        return;


//        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
//
//        if (field.nullableVersions().contains(version)) {
//            buffer.printf("if let Some(v) = &self.%s {%n", fieldName(field));
//            buffer.incrementIndent();
//            if (fieldFlexibleVersions(field).contains(version)) {
//                headerGenerator.addImport("varint_rs::VarintWriter");
//                buffer.printf("output.write_u32_varint((v.len() + 1) as u32)?;%n");
//            } else {
//                headerGenerator.addImport("byteorder::BigEndian");
//                headerGenerator.addImport("byteorder::WriteBytesExt");
//                buffer.printf("output.write_i32::<BigEndian>(v.len() as i32)?;%n");
//            }
//            buffer.printf("for elem in v {%n");
//            buffer.incrementIndent();
//            buffer.printf("elem.write(output)?%n");
//            buffer.decrementIndent();
//            buffer.printf("}%n");
//            buffer.decrementIndent();
//
//            buffer.printf("} else {%n");
//            buffer.incrementIndent();
//            if (fieldFlexibleVersions(field).contains(version)) {
//                headerGenerator.addImport("varint_rs::VarintWriter");
//                buffer.printf("output.write_u32_varint(0)?;%n");
//            } else {
//                headerGenerator.addImport("byteorder::BigEndian");
//                headerGenerator.addImport("byteorder::WriteBytesExt");
//                buffer.printf("output.write_i32::<BigEndian>(-1)?;%n");
//            }
//            buffer.decrementIndent();
//            buffer.printf("}%n");
//        } else {
//            if (fieldFlexibleVersions(field).contains(version)) {
//                headerGenerator.addImport("varint_rs::VarintWriter");
//                buffer.printf("output.write_u32_varint((self.%s.len() + 1) as u32)?;%n", fieldName(field));
//            } else {
//                headerGenerator.addImport("byteorder::BigEndian");
//                headerGenerator.addImport("byteorder::WriteBytesExt");
//                buffer.printf("output.write_i32::<BigEndian>(self.%s.len() as i32)?;%n", fieldName(field));
//            }
//            buffer.printf("for elem in self.%s {%n", fieldName(field));
//            buffer.incrementIndent();
//            buffer.printf("elem.write(output)?%n");
//            buffer.decrementIndent();
//            buffer.printf("}%n");
//        }


//        buffer.printf("let %s = {%n", fieldName(field));
//        buffer.incrementIndent();
//        buffer.printf("if arr_len < 0 {%n");
//        buffer.incrementIndent();
//        if (field.nullableVersions().contains(version)) {
//            buffer.printf("None%n");
//        } else {
//            headerGenerator.addImport("std::io::Error");
//            headerGenerator.addImport("std::io::ErrorKind");
//            buffer.printf("// TODO replace with proper error%n");
//            buffer.printf("return Err(Error::new(ErrorKind::Other, \"non-nullable field %s was serialized as null\"));%n",
//                fieldName(field));
//        }
//        buffer.decrementIndent();
//        buffer.printf("} else {%n");
//        buffer.incrementIndent();
//        if (arrayType.elementType().isArray()) {
//            throw new RuntimeException("Nested arrays are not supported.  " +
//                "Use an array of structures containing another array.");
//        } else {
//            buffer.printf("let mut vec: Vec<%s> = Vec::with_capacity(arr_len as usize);%n", rustType(arrayType.elementType(), headerGenerator));
//            buffer.printf("for _ in 0..arr_len {%n");
//            buffer.incrementIndent();
//
//
//            buffer.decrementIndent();
//            buffer.printf("}%n");
//            if (field.nullableVersions().contains(version)) {
//                buffer.printf("Some(vec)%n");
//            } else {
//                buffer.printf("vec%n");
//            }
//        }
//        buffer.decrementIndent();
//        buffer.printf("}%n");
//        buffer.decrementIndent();
//        buffer.printf("};%n");
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
