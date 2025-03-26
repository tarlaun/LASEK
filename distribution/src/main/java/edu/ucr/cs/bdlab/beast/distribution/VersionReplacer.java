package edu.ucr.cs.bdlab.beast.distribution;

import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class VersionReplacer {
  public static void main(String[] args) {
    String beastVersion = args.length == 0 ? "0.10.0-SNAPSHOT" : args[0];
    List<Path> paths = Arrays.asList(
        Paths.get("doc"),
        Paths.get("distribution/src/main/resources/archetype-resources/pom.xml"),
        Paths.get("distribution/src/main/resources/archetype-resources/bin/beast"),
        Paths.get("distribution/src/main/resources/archetype-resources/bin/beast.cmd")
        // Add more paths as needed
    );

    Map<String, String> replacements = new HashMap<>();
    // Define the replacements
    String versionRegex = "[\\d]+\\.[\\d]+\\.[\\d+](-(RC\\d|SNAPSHOT))?";
    replacements.put("\"beast-spark\" % \"[^\"]+\"", "\"beast-spark\" % \"" + beastVersion + "\"");
    replacements.put("-DarchetypeVersion=.*", "-DarchetypeVersion=" + beastVersion);
    replacements.put("beast-" + versionRegex, "beast-" + beastVersion);
    replacements.put("<artifactId>beast-spark</artifactId>(\\s*)<version>" + versionRegex + "</version>", "<artifactId>beast-spark</artifactId>$1<version>" + beastVersion + "</version>");
    replacements.put("<beast.version>" + versionRegex + "</beast.version>", "<beast.version>" + beastVersion + "</beast.version>");
    replacements.put("beast_version=" + versionRegex, "beast_version=" + beastVersion);

    for (Path path : paths) {
      if (Files.isDirectory(path)) {
        processDirectory(path, replacements);
      } else {
        processFile(path, replacements);
      }
    }
  }

  private static void processDirectory(Path dirPath, Map<String, String> replacements) {
    try (Stream<Path> paths = Files.walk(dirPath)) {
      paths.filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(".md"))
          .forEach(file -> processFile(file, replacements));
    } catch (IOException e) {
      System.err.println("An error occurred while processing directory " + dirPath + ": " + e.getMessage());
    }
  }

  private static void processFile(Path filePath, Map<String, String> replacements) {
    try {
      String content = new String(Files.readAllBytes(filePath));
      for (Map.Entry<String, String> entry : replacements.entrySet()) {
        content = content.replaceAll(entry.getKey(), entry.getValue());
      }
      Files.write(filePath, content.getBytes());
      System.out.println("Replacements done in file: " + filePath);
    } catch (IOException e) {
      System.err.println("An error occurred with file: " + filePath + " - " + e.getMessage());
    }
  }
}
