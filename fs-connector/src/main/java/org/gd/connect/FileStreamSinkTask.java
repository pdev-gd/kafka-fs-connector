// FileStreamSinkTask.java

package org.gd.connect;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class FileStreamSinkTask extends SinkTask {
    private String filename;
    private String filenamePrefix;
    private long maxSize;
    private long currentSize;
    private PrintWriter writer;  // New instance variable

    private static final String FILE_CONFIG = "file";
    private static final String MAX_SIZE_CONFIG = "max.size";

    public String getFilename() {
        return filename;
    }

    @Override
    public String version() {
        return new FileStreamSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        filenamePrefix = props.get(FILE_CONFIG);
        filename = generateFilename();

        String maxSizeStr = props.get(MAX_SIZE_CONFIG);
        if (maxSizeStr != null) {
            maxSize = Long.parseLong(maxSizeStr);
        } else {
            // Handle the case when maxSizeStr is null, for example, set maxSize to a default value.
            maxSize = 1024; // Default value
        }
        currentSize = 0;

        // Initialize the PrintWriter
        try {
            this.writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open the PrintWriter", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            String logEntry = String.format("Topic: %s, Key: %s, Value: %s", record.topic(), record.key(), record.value());
            if (currentSize + logEntry.length() > maxSize) {
                rollover();
            }
            writer.println(logEntry);  // Use the instance PrintWriter
            currentSize += logEntry.length();
        }
        writer.flush();  // Ensure that everything is written to file
    }

    private void rollover() {
        writer.close();  // Close the existing writer
        filename = generateFilename(); // Get new filename with current timestamp

        try {
            // Open a new PrintWriter for the new file
            writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
        } catch (IOException e) {
            throw new RuntimeException("File rollover failed", e);
        }

        currentSize = 0;
    }

    private String generateFilename() {
        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmssSSS").format(new Date());
        return String.format("%s-%s.log", filenamePrefix, timestamp);
    }

    @Override
    public void stop() {
        writer.close();  // Don't forget to close the writer here as well!
    }
}
