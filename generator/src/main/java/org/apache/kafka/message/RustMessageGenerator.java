package org.apache.kafka.message;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class RustMessageGenerator {
    private static final String RUST_SUFFIX = ".rs";
    private static final String MOD_RS = "mod" + RUST_SUFFIX;

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("message-generator")
                .defaultHelp(true)
                .description("The Kafka message generator");
        parser.addArgument("--output", "-o")
                .action(store())
                .required(true)
                .metavar("OUTPUT")
                .help("The output directory to create.");
        parser.addArgument("--input", "-i")
                .action(store())
                .required(true)
                .metavar("INPUT")
                .help("The input directory to use.");
        Namespace res = parser.parseArgsOrFail(args);
        process(res.getString("output"), res.getString("input"));
    }

    private static void process(String outputDir, String inputDir) throws Exception {
        Files.createDirectories(Paths.get(outputDir));

        List<String> messageTypeMods = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files
                .newDirectoryStream(Paths.get(inputDir), MessageGenerator.JSON_GLOB)) {
            for (Path inputPath : directoryStream) {
                messageTypeMods.add(processJson(outputDir, inputPath));
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputDir, MOD_RS), StandardCharsets.UTF_8)) {
            for (String messageTypeMod : messageTypeMods) {
                writer.write(String.format("pub mod %s;%n", messageTypeMod));
            }
        }

    }

    private static String processJson(String outputDir, Path inputPath) throws Exception {
        MessageSpec spec = MessageGenerator.JSON_SERDE.readValue(inputPath.toFile(), MessageSpec.class);

        String className = spec.dataClassName();
        if (className.endsWith("Data")) {
            className = className.substring(0, className.length() - 4);
        }
        final String messageTypeMod = MessageGenerator.toSnakeCase(className);
        Path messageTypeModDir = Paths.get(outputDir, messageTypeMod);
        Files.createDirectories(messageTypeModDir);

        List<String> versionMods = new ArrayList<>();
        for (short version = spec.validVersions().lowest(); version <= spec.validVersions().highest(); version++) {
            versionMods.add(processVersion(messageTypeModDir, spec, version));
        }

        try (BufferedWriter writer = Files.newBufferedWriter(messageTypeModDir.resolve(MOD_RS), StandardCharsets.UTF_8)) {
            for (String versionMod : versionMods) {
                writer.write(String.format("pub mod %s;%n", versionMod));
            }
        }

        return messageTypeMod;
    }

    private static String processVersion(Path messageTypeModDir, MessageSpec spec, short version) throws Exception {
        final String versionMod = String.format("v%d", version);

        final RustMessageDataGenerator generator = new RustMessageDataGenerator(spec, version);
        try (BufferedWriter writer = Files.newBufferedWriter(messageTypeModDir.resolve(versionMod + RUST_SUFFIX), StandardCharsets.UTF_8)) {
            generator.generateAndWrite(writer);
        }

        return versionMod;
    }
}
