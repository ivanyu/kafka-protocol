package org.apache.kafka.message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RustHeaderGenerator {
    final CodeBuffer buffer = new CodeBuffer();
    private final Set<Import> imports = new HashSet<>();

    public void addImport(String fqn) {
        this.imports.add(new Import(fqn, ImportCfg.NORMAL));
    }

    public void addImportTest(String fqn) {
        this.imports.add(new Import(fqn, ImportCfg.TEST));
    }

    public void addImportNonTest(String fqn) {
        this.imports.add(new Import(fqn, ImportCfg.NON_TEST));
    }

    public void generate() {
        Map<PackageAndImportCfg, List<Import>> importsByPackageAndTestFlag = new HashMap<>();
        for (Import imp : imports) {
            importsByPackageAndTestFlag.computeIfAbsent(
                new PackageAndImportCfg(imp.pkg, imp.importCfg),
                k -> new ArrayList<>()
            ).add(imp);
        }

        outputByPredicate(importsByPackageAndTestFlag, e -> e.startsWith("std::"));
        outputByPredicate(importsByPackageAndTestFlag, e -> !e.startsWith("std::") && !e.startsWith("crate::"));
        outputByPredicate(importsByPackageAndTestFlag, e -> e.startsWith("crate::"));
    }

    private void outputByPredicate(
        Map<PackageAndImportCfg, List<Import>> importsByPackageAndTestFlag,
        Predicate<String> packagePredicate
    ) {
        boolean outputSomething = false;

        for (ImportCfg importCfg : new ImportCfg[] { ImportCfg.NORMAL, ImportCfg.NON_TEST, ImportCfg.TEST }) {
            List<Map.Entry<PackageAndImportCfg, List<Import>>> imports = importsByPackageAndTestFlag.entrySet().stream()
                .filter(e -> e.getKey().importCfg == importCfg && packagePredicate.test(e.getKey().pkg))
                .sorted(Comparator.comparing(e -> e.getKey().pkg))
                .collect(Collectors.toList());
            for (Map.Entry<PackageAndImportCfg, List<Import>> entry : imports) {
                outputPackage(entry, importCfg);
            }
            outputSomething = outputSomething || !imports.isEmpty();
        }
        if (outputSomething) {
            buffer.printf("%n");
        }
    }

    private void outputPackage(Map.Entry<PackageAndImportCfg, List<Import>> entry, ImportCfg importCfg) {
        List<Import> imports = entry.getValue().stream()
            .sorted(Comparator.comparing(o -> o.symbol))
            .collect(Collectors.toList());
        String cfgPrefix = "";
        if (importCfg == ImportCfg.TEST) {
            cfgPrefix = "#[cfg(test)] ";
        } else if (importCfg == ImportCfg.NON_TEST) {
            cfgPrefix = "#[cfg(not(test))] ";
        }
        String pkg = entry.getKey().pkg;
        if (imports.size() == 1) {
            buffer.printf("%suse %s::%s;%n", cfgPrefix, pkg, imports.get(0).symbol);
        } else if (imports.size() > 1) {
            buffer.printf("%suse %s::{%s};%n", cfgPrefix, pkg,
                imports.stream().map(o -> o.symbol).collect(Collectors.joining(", ")));
        }
    }

    private enum ImportCfg {
        NORMAL,
        TEST,
        NON_TEST
    }

    private static class Import {
        final String pkg;
        final String symbol;
        final ImportCfg importCfg;

        private Import(String fqn, ImportCfg importCfg) {
            int i = fqn.lastIndexOf("::");
            if (i == -1) {
                throw new RuntimeException("Unexpected");
            }

            this.pkg = fqn.substring(0, i);
            this.symbol = fqn.substring(i + 2);
            this.importCfg = importCfg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Import anImport = (Import) o;
            return Objects.equals(pkg, anImport.pkg) && Objects.equals(symbol, anImport.symbol) && importCfg == anImport.importCfg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pkg, symbol, importCfg);
        }
    }

    private static class PackageAndImportCfg {
        final String pkg;
        final ImportCfg importCfg;

        private PackageAndImportCfg(String pkg, ImportCfg importCfg) {
            this.pkg = pkg;
            this.importCfg = importCfg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PackageAndImportCfg that = (PackageAndImportCfg) o;
            return Objects.equals(pkg, that.pkg) && importCfg == that.importCfg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pkg, importCfg);
        }
    }
}
