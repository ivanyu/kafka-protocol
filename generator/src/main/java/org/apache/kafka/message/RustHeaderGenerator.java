package org.apache.kafka.message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RustHeaderGenerator {
    final CodeBuffer buffer = new CodeBuffer();
    private final Set<String> imports = new HashSet<>();

    public void addImport(String newImport) {
        this.imports.add(newImport);
    }

    public void generate() {
        Map<String, List<String>> importsByPackage = new TreeMap<>();
        for (String imp : imports) {
            int i = imp.lastIndexOf("::");
            if (i == -1) {
                throw new RuntimeException("Unexpected");
            }

            String prefix = imp.substring(0, i);
            String symbol = imp.substring(i + 2);
            importsByPackage.computeIfAbsent(prefix, k -> new ArrayList<>()).add(symbol);
        }

        List<Map.Entry<String, List<String>>> stdImports = importsByPackage.entrySet().stream()
                .filter(e -> e.getKey().startsWith("std::"))
                .collect(Collectors.toList());
        stdImports.forEach(this::outputPackage);
        if (!stdImports.isEmpty()) {
            buffer.printf("%n");
        }

        List<Map.Entry<String, List<String>>> otherImports = importsByPackage.entrySet().stream()
                .filter(e -> !e.getKey().startsWith("std::") && !e.getKey().startsWith("crate::"))
                .collect(Collectors.toList());
        otherImports.forEach(this::outputPackage);
        if (!otherImports.isEmpty()) {
            buffer.printf("%n");
        }

        List<Map.Entry<String, List<String>>> crateImports = importsByPackage.entrySet().stream()
                .filter(e -> e.getKey().startsWith("crate::"))
                .collect(Collectors.toList());
        crateImports.forEach(this::outputPackage);
        buffer.printf("%n");
    }

    private void outputPackage(Map.Entry<String, List<String>> entry) {
        String prefix = entry.getKey();
        List<String> symbols = entry.getValue();
        symbols.sort(String::compareTo);
        if (symbols.size() == 1) {
            buffer.printf("use %s::%s;%n", prefix, symbols.get(0));
        } else {
            buffer.printf("use %s::{%s};%n", prefix, String.join(", ", symbols));
        }
    }
}
