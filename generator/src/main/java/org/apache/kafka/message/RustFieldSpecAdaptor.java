package org.apache.kafka.message;

class RustFieldSpecAdaptor {
    private final FieldSpec fieldSpec;
    private final short version;
    private final RustHeaderGenerator headerGenerator;

    RustFieldSpecAdaptor(FieldSpec fieldSpec, short version, RustHeaderGenerator headerGenerator) {
        this.fieldSpec = fieldSpec;
        this.version = version;
        this.headerGenerator = headerGenerator;
    }

    String fieldDefault() {
        HeaderGenerator fakeHeaderGenerator = new HeaderGenerator("");

        FieldType type = fieldSpec.type();
        if (type instanceof FieldType.BoolFieldType) {
            return fieldSpec.fieldDefault(fakeHeaderGenerator, null);
        } else if ((type instanceof FieldType.Int8FieldType) ||
                (type instanceof FieldType.Int16FieldType) ||
                (type instanceof FieldType.Uint16FieldType) ||
                (type instanceof FieldType.Uint32FieldType) ||
                (type instanceof FieldType.Int32FieldType) ||
                (type instanceof FieldType.Int64FieldType)) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);

            if (type instanceof FieldType.Int8FieldType) {
                result = result.substring("(byte) ".length()) + "_i8";
            } else if (type instanceof FieldType.Int16FieldType) {
                result = result.substring("(short) ".length()) + "_i16";
            } else if (type instanceof FieldType.Uint16FieldType) {
                result = result + "_u16";
            } else if (type instanceof FieldType.Uint32FieldType) {
                result = result + "_u32";
            } else if (type instanceof FieldType.Int32FieldType) {
                result = result + "_i32";
            } else if (type instanceof FieldType.Int64FieldType) {
                result = result.substring(0, result.length() - 1) + "_i64";  // trim "L"
            }

            return result;
        } else if (type instanceof FieldType.UUIDFieldType) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);
            if (result.equals("Uuid.ZERO_UUID")) {
                return "Uuid::nil()";
            } else {
                throw new RuntimeException("not supported yet");
            }
        } else if (type instanceof FieldType.Float64FieldType) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);
            if (!result.equals("0.0")) {
                result = result.replace("Double.parseDouble", "f64::from_str")
                        + ".unwrap()";
            }
            return result;
        } else if (type instanceof FieldType.StringFieldType) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);
            if (result.equals("null")) {
                return "None";
            } else {
                result = "String::from(" + result + ")";
                if (fieldSpec.nullableVersions().contains(version)) {
                    return "Some(" + result + ")";
                }
                return result;
            }
        } else if (type.isBytes()) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);
            if (result.equals("null")) {
                return "None";
            } else if (result.equals("Bytes.EMPTY") || result.equals("ByteUtils.EMPTY_BUF")) {
                result = "Vec::new()";
                if (fieldSpec.nullableVersions().contains(version)) {
                    result = "Some(" + result + ")";
                }
                return result;
            } else {
                throw new RuntimeException("not supported yet");
            }
        } else if (type.isRecords()) {
            throw new RuntimeException("not supported yet");
        } else if (type.isStruct()) {
            String result = fieldSpec.fieldDefault(fakeHeaderGenerator, null);
            if (result.equals("null")) {
                return "None";
            } else {
                result = result.substring("new ".length());
                result = result.replace("()", "::default()");
                return result;
            }
        } else if (type.isArray()) {
            if (fieldSpec.defaultString().equals("null")) {
                fieldSpec.fieldDefault(fakeHeaderGenerator, null);
                return "None";
            } else {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                String result = String.format("Vec::<%s>::new()", rustType(arrayType.elementType(), headerGenerator));
                if (fieldSpec.nullableVersions().contains(version)) {
                    result = "Some(" + result + ")";
                }
                return result;
            }
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    static String rustType(FieldType type, RustHeaderGenerator headerGenerator) {
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
            throw new RuntimeException("not supported yet");
        } else if (type.isStruct()) {
            return MessageGenerator.capitalizeFirst(type.toString());
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            return String.format("Vec<%s>", rustType(arrayType.elementType(), headerGenerator));
        } else {
            throw new RuntimeException("Unknown field type " + type);
        }
    }

    void generateNonDefaultValueCheck(CodeBuffer buffer,
                                      String fieldPrefix) {
        HeaderGenerator fakeHeaderGenerator = new HeaderGenerator("");
        if (fieldSpec.type().isArray()) {
            if (fieldSpec.nullableVersions().empty()) {
                buffer.printf("if !%s%s.is_empty() {%n", fieldPrefix, fieldName());
            } else {
                throw new RuntimeException("not supported yet");
            }
        } else if (fieldSpec.type().isBytes()) {
            throw new RuntimeException("not supported yet");
        } else if (fieldSpec.type().isString() || fieldSpec.type().isStruct() || fieldSpec.type() instanceof FieldType.UUIDFieldType) {
            if (fieldDefault().equals("None")) {
                buffer.printf("if %s%s.is_some() {%n", fieldPrefix, fieldName());
            } else if (fieldSpec.nullableVersions().empty()) {
                buffer.printf("if %s%s != %s {%n",
                    fieldPrefix, fieldName(), fieldDefault());
            } else {
                buffer.printf("if %s%s.is_none() || %s%s != %s {%n",
                    fieldPrefix, fieldName(), fieldPrefix, fieldName(),
                    fieldDefault());
            }
        } else if (fieldSpec.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if %s%s%s {%n",
                fieldDefault().equals("true") ? "!" : "",
                fieldPrefix, fieldName());
        } else {
            buffer.printf("if %s%s != %s {%n",
                fieldPrefix, fieldName(), fieldDefault());
        }
    }

    String fieldName() {
        String snakeCaseName = fieldSpec.snakeCaseName();
        if (snakeCaseName.equals("type")) {
            return "type_";
        } else if (snakeCaseName.equals("match")) {
            return "match_";
        } else {
            return snakeCaseName;
        }
    }
}
