package org.apache.kafka.message;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class RustApiMessageTypeGenerator extends ApiMessageTypeGenerator {
    private final RustHeaderGenerator headerGenerator = new RustHeaderGenerator();
    private final CodeBuffer buffer = new CodeBuffer();

    public RustApiMessageTypeGenerator() {
        super("");
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        generate();
        write(writer);
    }

    private void generate() throws IOException {
        buffer.printf("pub struct ApiMessageType {%n");
        buffer.incrementIndent();
        buffer.printf("pub name: &'static str,%n");
        buffer.printf("pub api_key: i16,%n");
        buffer.printf("pub lowest_supported_version: i16,%n");
        buffer.printf("pub highest_supported_version: i16,%n");
        buffer.printf("pub lowest_deprecated_version: i16,%n");
        buffer.printf("pub highest_deprecated_version: i16,%n");
        buffer.printf("pub latest_version_unstable: bool,%n");
        buffer.decrementIndent();
        buffer.printf("}%n");

        buffer.printf("%n");

        buffer.printf("impl ApiMessageType {%n");
        buffer.incrementIndent();

        generateEnumValues();

        buffer.printf("%n");

        generateHeaderVersion("request");

        buffer.printf("%n");

        generateHeaderVersion("response");

        buffer.decrementIndent();
        buffer.printf("}%n");

        buffer.printf("%n");

        generateTests();
    }

    private void generateEnumValues() {
        // Not a technically correct name for Rust, but to follow the pattern in the parent.
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData apiData = entry.getValue();
            String name = apiData.name();

            buffer.printf("pub const %s: ApiMessageType = ApiMessageType { name: \"%s\", api_key: %d, lowest_supported_version: %d, highest_supported_version: %d, lowest_deprecated_version: %d, highest_deprecated_version: %d, latest_version_unstable: %s };%n",
                MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT),
                MessageGenerator.capitalizeFirst(name),
                entry.getKey(),
                apiData.requestSpec.struct().versions().lowest(),
                apiData.requestSpec.struct().versions().highest(),
                apiData.requestSpec.struct().deprecatedVersions().lowest(),
                apiData.requestSpec.struct().deprecatedVersions().highest(),
                apiData.requestSpec.latestVersionUnstable()
            );
        }
    }

    private void generateHeaderVersion(String type) throws IOException {
        buffer.printf("pub fn %s_header_version(&self, _version: i16) -> i16 {%n", type);
        buffer.incrementIndent();
        buffer.printf("match self.api_key {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            short apiKey = entry.getKey();
            ApiData apiData = entry.getValue();
            String name = apiData.name();
            buffer.printf("%d => {  // %s%n", apiKey, MessageGenerator.capitalizeFirst(name));
            buffer.incrementIndent();
            if (type.equals("response") && apiKey == 18) {
                buffer.printf("// ApiVersionsResponse always includes a v0 header.%n");
                buffer.printf("// See KIP-511 for details.%n");
                buffer.printf("0%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
                continue;
            }
            if (type.equals("request") && apiKey == 7) {
                buffer.printf("// Version 0 of ControlledShutdownRequest has a non-standard request header%n");
                buffer.printf("// which does not include clientId.  Version 1 of ControlledShutdownRequest%n");
                buffer.printf("// and later use the standard request header.%n");
                buffer.printf("if _version == 0 {%n");
                buffer.incrementIndent();
                buffer.printf("0%n");
                buffer.decrementIndent();
                buffer.printf("} else ");
            }
            ApiData data = entry.getValue();
            MessageSpec spec;
            if (type.equals("request")) {
                spec = data.requestSpec;
            } else if (type.equals("response")) {
                spec = data.responseSpec;
            } else {
                throw new RuntimeException("Invalid type " + type + " for generateHeaderVersion");
            }
            if (spec == null) {
                throw new RuntimeException("failed to find " + type + " for API key " + apiKey);
            }

            CodeBuffer tmpBuffer = new CodeBuffer();
            VersionConditional.forVersions(spec.flexibleVersions(),
                spec.validVersions()).
                ifMember(__ -> {
                    if (type.equals("request")) {
                        tmpBuffer.printf("2%n");
                    } else {
                        tmpBuffer.printf("1%n");
                    }
                }).
                ifNotMember(__ -> {
                    if (type.equals("request")) {
                        tmpBuffer.printf("1%n");
                    } else {
                        tmpBuffer.printf("0%n");
                    }
                }).generate(tmpBuffer);

            StringWriter condStringWriter = new StringWriter();
            tmpBuffer.write(condStringWriter);
            String cond = condStringWriter.toString().replace("\n", "")
                    .replace("if (_version", "if _version");
            cond = Pattern.compile("(\\d)\\) \\{").matcher(cond).replaceAll("$1 {");
            cond = Pattern.compile("(\\d)}").matcher(cond).replaceAll("$1 }");
            cond = Pattern.compile("\\s+").matcher(cond).replaceAll(" ");
            buffer.printf("%s%n", cond);

            buffer.decrementIndent();
            buffer.printf("}%n%n");
        }
        buffer.printf("_ => {%n");
        buffer.incrementIndent();
        buffer.printf("panic!(\"Unsupported API key {}\", self.api_key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");

        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateTests() {
        buffer.printf("#[cfg(test)]%n");
        buffer.printf("mod tests {%n");
        buffer.incrementIndent();

        buffer.printf("use super::*;%n%n");

        buffer.printf("#[test]%n");
        buffer.printf("fn test_header_version() {%n");
        buffer.incrementIndent();

        buffer.printf("assert_eq!(ApiMessageType::PRODUCE.request_header_version(0), 1);%n");
        buffer.printf("assert_eq!(ApiMessageType::PRODUCE.response_header_version(0), 0);%n");
        buffer.printf("%n");

        buffer.printf("assert_eq!(ApiMessageType::PRODUCE.request_header_version(1), 1);%n");
        buffer.printf("assert_eq!(ApiMessageType::PRODUCE.response_header_version(1), 0);%n");
        buffer.printf("%n");

        buffer.printf("assert_eq!(ApiMessageType::CONTROLLED_SHUTDOWN.request_header_version(0), 0);%n");
        buffer.printf("assert_eq!(ApiMessageType::CONTROLLED_SHUTDOWN.response_header_version(0), 0);%n");
        buffer.printf("%n");

        buffer.printf("assert_eq!(ApiMessageType::CONTROLLED_SHUTDOWN.request_header_version(1), 1);%n");
        buffer.printf("assert_eq!(ApiMessageType::CONTROLLED_SHUTDOWN.response_header_version(1), 0);%n");
        buffer.printf("%n");

        buffer.printf("assert_eq!(ApiMessageType::CREATE_TOPICS.request_header_version(4), 1);%n");
        buffer.printf("assert_eq!(ApiMessageType::CREATE_TOPICS.response_header_version(4), 0);%n");
        buffer.printf("%n");

        buffer.printf("assert_eq!(ApiMessageType::CREATE_TOPICS.request_header_version(5), 2);%n");
        buffer.printf("assert_eq!(ApiMessageType::CREATE_TOPICS.response_header_version(5), 1);%n");
        buffer.printf("%n");

        buffer.decrementIndent();
        buffer.printf("}%n");

        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer.write(writer);
        buffer.write(writer);
    }
}
