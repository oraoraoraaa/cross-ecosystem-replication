package com.higolab;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.maven.index.reader.ChunkReader;
import org.apache.maven.index.reader.Record;
import org.apache.maven.index.reader.ResourceHandler;
import org.apache.maven.index.reader.WritableResourceHandler;
import org.apache.maven.model.Model;
import org.apache.maven.model.Scm;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MavenCentralMiner {
    
    private static final String MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2/.index/";
    private static final String POM_BASE_URL = "https://repo1.maven.org/maven2/";
    private static final int THREAD_POOL_SIZE = 50;
    private static final int POM_TIMEOUT_MS = 10000;
    
    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("Maven Central Package Miner - Complete Solution");
        System.out.println("Using Apache Maven Indexer");
        System.out.println("=".repeat(80));
        System.out.println();
        
        // Setup local temp directory in the script folder
        Path tempDir = Paths.get("temp").toAbsolutePath();
        
        try {
            // Clean up any existing temp directory first
            if (Files.exists(tempDir)) {
                System.out.println("Cleaning up existing temporary directory...");
                deleteDirectory(tempDir.toFile());
            }
            
            // Create fresh temp directory
            Files.createDirectories(tempDir);
            
            Path outputPath = Paths.get("../../../data/package-lists/Maven.csv").toAbsolutePath().normalize();
            Files.createDirectories(outputPath.getParent());
            
            System.out.println("Temporary directory: " + tempDir);
            System.out.println("Output file: " + outputPath);
            System.out.println();
            
            // Download and read Maven Central index
            System.out.println("Step 1: Downloading Maven Central index...");
            List<ArtifactInfo> artifacts = readMavenIndex(tempDir);
            System.out.println("Found " + artifacts.size() + " artifacts in index");
            System.out.println();
            
            // Fetch POM information for artifacts
            System.out.println("Step 2: Fetching POM metadata...");
            enrichWithPomData(artifacts);
            System.out.println();
            
            // Write to CSV
            System.out.println("Step 3: Writing results to CSV...");
            writeToCSV(artifacts, outputPath);
            System.out.println();
            
            System.out.println("=".repeat(80));
            System.out.println("Mining completed successfully!");
            System.out.println("Total packages: " + artifacts.size());
            System.out.println("Output: " + outputPath);
            System.out.println("=".repeat(80));
            
        } catch (Exception e) {
            System.err.println("Error during mining: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            // Always cleanup temp directory
            if (Files.exists(tempDir)) {
                System.out.println();
                System.out.println("Cleaning up temporary directory...");
                deleteDirectory(tempDir.toFile());
                System.out.println("Cleanup completed.");
            }
        }
    }
    
    private static List<ArtifactInfo> readMavenIndex(Path tempDir) throws IOException {
        // Use a Set to track unique artifacts (groupId:artifactId) to avoid duplicates
        Set<String> seenArtifacts = new HashSet<>();
        List<ArtifactInfo> artifacts = new ArrayList<>();
        
        // Create a simple resource handler for HTTP downloads
        ResourceHandler resourceHandler = new ResourceHandler() {
            @Override
            public Resource locate(String name) throws IOException {
                System.out.println("Downloading: " + name);
                URL url = new URL(MAVEN_CENTRAL_URL + name);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(300000);
                
                if (conn.getResponseCode() == 200) {
                    long contentLength = conn.getContentLengthLong();
                    if (contentLength > 0) {
                        System.out.println("  Size: " + (contentLength / 1024 / 1024) + " MB");
                    }
                    
                    File tempFile = new File(tempDir.toFile(), name);
                    tempFile.getParentFile().mkdirs();
                    
                    try (InputStream in = conn.getInputStream();
                         FileOutputStream out = new FileOutputStream(tempFile)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        long totalRead = 0;
                        long lastReport = 0;
                        while ((bytesRead = in.read(buffer)) != -1) {
                            out.write(buffer, 0, bytesRead);
                            totalRead += bytesRead;
                            // Report progress every 10MB
                            if (totalRead - lastReport > 10 * 1024 * 1024) {
                                System.out.println("  Downloaded: " + (totalRead / 1024 / 1024) + " MB");
                                lastReport = totalRead;
                            }
                        }
                    }
                    System.out.println("  Completed: " + name);
                    
                    return new FileResource(tempFile);
                }
                
                throw new IOException("Failed to download: " + name + " (HTTP " + conn.getResponseCode() + ")");
            }
        };
        
        // Read the index
        try (org.apache.maven.index.reader.IndexReader indexReader = 
                new org.apache.maven.index.reader.IndexReader(null, resourceHandler)) {
            
            int chunkCount = 0;
            int recordCount = 0;
            for (ChunkReader chunkReader : indexReader) {
                chunkCount++;
                System.out.println("Reading chunk: " + chunkReader.getName());
                System.out.println("Chunk version: " + chunkReader.getVersion());
                
                int chunkRecords = 0;
                for (Map<String, String> doc : chunkReader) {
                    recordCount++;
                    chunkRecords++;
                    
                    // Debug: print first few records to see what fields are available
                    if (recordCount <= 5) {
                        System.out.println("Sample record " + recordCount + " keys: " + doc.keySet());
                        for (String key : doc.keySet()) {
                            System.out.println("  " + key + " = " + doc.get(key));
                        }
                    }
                    
                    String uInfo = doc.get("u");
                    if (uInfo != null) {
                        // Parse the format: groupId|artifactId|version|classifier|extension
                        String[] parts = uInfo.split("\\|");
                        if (parts.length >= 3) {
                            String groupId = parts[0];
                            String artifactId = parts[1];
                            String version = parts[2];
                            
                            if (groupId != null && !groupId.isEmpty() && 
                                artifactId != null && !artifactId.isEmpty() && 
                                version != null && !version.isEmpty()) {
                                
                                // Only keep one version per artifact (groupId:artifactId)
                                String artifactKey = groupId + ":" + artifactId;
                                if (!seenArtifacts.contains(artifactKey)) {
                                    seenArtifacts.add(artifactKey);
                                    artifacts.add(new ArtifactInfo(groupId, artifactId, version));
                                }
                            }
                        }
                    }
                    
                    // Clear the document map to free memory
                    doc.clear();
                    
                    // Periodically suggest GC for large datasets
                    if (recordCount % 100000 == 0) {
                        System.out.println("Processed " + recordCount + " records, found " + artifacts.size() + " unique artifacts");
                        System.gc();
                    }
                }
                System.out.println("Chunk had " + chunkRecords + " records");
            }
            System.out.println("Total chunks read: " + chunkCount);
            System.out.println("Total records processed: " + recordCount);
        }
        
        return artifacts;
    }
    
    private static void enrichWithPomData(List<ArtifactInfo> artifacts) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        AtomicInteger processed = new AtomicInteger(0);
        int total = artifacts.size();
        
        System.out.println("Processing " + total + " artifacts with " + THREAD_POOL_SIZE + " threads...");
        
        List<Future<?>> futures = new ArrayList<>();
        
        for (ArtifactInfo artifact : artifacts) {
            futures.add(executor.submit(() -> {
                try {
                    fetchPomInfo(artifact);
                } catch (Exception e) {
                    // Silently fail for individual artifacts
                }
                
                int count = processed.incrementAndGet();
                if (count % 1000 == 0) {
                    System.out.printf("Progress: %d / %d (%.1f%%)%n", 
                        count, total, (count * 100.0 / total));
                }
            }));
        }
        
        // Wait for all tasks to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // Continue processing
            }
        }
        
        executor.shutdown();
        System.out.println("Completed processing all artifacts");
    }
    
    private static void fetchPomInfo(ArtifactInfo artifact) {
        String groupPath = artifact.groupId.replace('.', '/');
        String pomUrl = String.format("%s%s/%s/%s/%s-%s.pom",
            POM_BASE_URL, groupPath, artifact.artifactId, artifact.version,
            artifact.artifactId, artifact.version);
        
        try {
            URL url = new URL(pomUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(POM_TIMEOUT_MS);
            conn.setReadTimeout(POM_TIMEOUT_MS);
            
            if (conn.getResponseCode() == 200) {
                try (InputStream in = conn.getInputStream()) {
                    MavenXpp3Reader reader = new MavenXpp3Reader();
                    Model model = reader.read(in);
                    
                    if (model.getUrl() != null && !model.getUrl().trim().isEmpty()) {
                        artifact.homepageUrl = model.getUrl().trim();
                    }
                    
                    if (model.getScm() != null && model.getScm().getUrl() != null) {
                        artifact.repositoryUrl = model.getScm().getUrl().trim();
                    }
                }
            }
        } catch (Exception e) {
            // Silently fail - keep default "nan" values
        }
    }
    
    private static void writeToCSV(List<ArtifactInfo> artifacts, Path outputPath) throws IOException {
        try (Writer writer = Files.newBufferedWriter(outputPath);
             CSVPrinter csv = new CSVPrinter(writer, CSVFormat.DEFAULT
                 .withHeader("ID", "Platform", "Name", "Homepage URL", "Repository URL"))) {
            
            int id = 1;
            for (ArtifactInfo artifact : artifacts) {
                csv.printRecord(
                    id++,
                    "Maven",
                    artifact.groupId + ":" + artifact.artifactId,
                    artifact.homepageUrl,
                    artifact.repositoryUrl
                );
            }
        }
        
        System.out.println("CSV file written successfully");
    }
    
    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
    
    // Inner classes
    
    static class ArtifactInfo {
        String groupId;
        String artifactId;
        String version;
        String homepageUrl = "nan";
        String repositoryUrl = "nan";
        
        ArtifactInfo(String groupId, String artifactId, String version) {
            this.groupId = groupId;
            this.artifactId = artifactId;
            this.version = version;
        }
    }
    
    static class FileResource implements ResourceHandler.Resource {
        private final File file;
        
        FileResource(File file) {
            this.file = file;
        }
        
        @Override
        public InputStream read() throws IOException {
            return new FileInputStream(file);
        }
        
        @Override
        public void close() throws IOException {
            // Nothing to close
        }
    }
}
