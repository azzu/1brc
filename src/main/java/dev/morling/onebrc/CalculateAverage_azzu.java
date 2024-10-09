package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CalculateAverage_azzu {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {

        float min;
        float max;
        int count;
        float sum;

        // Measurement() {
        // }

        public Measurement(float val) {
            this.min = val;
            this.max = val;
            this.count = 1;
            this.sum = val;
        }

        public void add(float val) {
            add(val, val, 1, val);
        }

        public void add(float min, float max, int count, float sum) {
            this.min = Math.min(this.min, min);
            this.max = Math.max(this.max, max);
            this.count += count;
            this.sum += sum;
        }

        public String toString() {
            return "%s/%s/%s".formatted(this.min, Math.round((this.sum / this.count) * 10) / 10.0, this.max);
        }

        public void merge(Measurement other) {
            add(other.min, other.max, other.count, other.sum);
        }
    }

    public static void main() throws IOException, ExecutionException, InterruptedException {
        long fileSize = new File(FILE).length();
        System.out.println(MessageFormat.format("FILE SIZE: {0} bytes", fileSize));
        int processors = Runtime.getRuntime().availableProcessors();
        System.out.println(MessageFormat.format("PROCESSORS: {0}", processors));
        int readSegmentSize = (int) Math.min(Integer.MAX_VALUE, (fileSize / processors));
        int numOfSection = (int) (fileSize / readSegmentSize);
        System.out.println(MessageFormat.format("{0} bytes per read, {1} sections", readSegmentSize, numOfSection));

        ExecutorService executorService = Executors.newFixedThreadPool(processors);
        List<CompletableFuture<Map<String, Measurement>>> futures = new ArrayList<>();

        for (int i = 0; i < numOfSection; i++) {
            long byteStart = (long) i * readSegmentSize;
            long byteEnd = Math.min(fileSize, (byteStart + readSegmentSize + 100));

            // System.out.println("[" + i +"] START : " + byteStart + ", END : " + (byteEnd - byteStart));
            CompletableFuture<Map<String, Measurement>> completableFuture;
            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(new File(FILE).toPath(), StandardOpenOption.READ)) {

                completableFuture = CompletableFuture.supplyAsync(() -> {
                    MappedByteBuffer mappedByteBuffer;
                    try {
                        mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, byteStart, (byteEnd - byteStart));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (byteStart > 0) {
                        while (mappedByteBuffer.get() != '\n')
                            ;
                    }

                    Map<String, Measurement> measurements = new HashMap<>();
                    while (mappedByteBuffer.position() < readSegmentSize) {
                        String station = getStation(mappedByteBuffer);
                        float temperature = getTemperature(mappedByteBuffer);
                        if (measurements.containsKey(station)) {
                            measurements.get(station).add(temperature);
                        } else {
                            measurements.put(station, new Measurement(temperature));
                        }
                    }
                    return measurements;
                }, executorService);
            }
            futures.add(completableFuture);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        Map<String, Measurement> treeMap = new TreeMap<>();
        for (CompletableFuture<Map<String, Measurement>> future : futures) {
            Map<String, Measurement> measurementMap = future.get();
            measurementMap.forEach((key, measurement) -> {
                if (treeMap.containsKey(key)) {
                    treeMap.get(key).merge(treeMap.get(key));
                }
                else {
                    treeMap.put(key, measurement);
                }
            });
        }
        System.out.println(treeMap);
        System.exit(0);
    }

    private static String getStation(final MappedByteBuffer mappedByteBuffer) {
        byte currByte;
        int byteCount = 0;
        byte[] bytes = new byte[100];
        while ((currByte = mappedByteBuffer.get()) != ';') {
            bytes[byteCount++] = currByte;
        }
        String val = new String(bytes, 0, byteCount, StandardCharsets.UTF_8);
        // System.out.println("STATION: " + val);
        return val;
    }

    private static float getTemperature(final MappedByteBuffer mappedByteBuffer) {
        String value;
        // long value = 0;
        byte[] bytes = new byte[4];
        mappedByteBuffer.get(bytes);

        if (bytes[1] == '.') { // n.n
            value = new String(bytes, StandardCharsets.UTF_8);
            // value = value.replace("\n", "").replace("\r", "");
        }
        else {
            if (bytes[3] == '.') { // -nn.n
                byte[] addBytes = new byte[1];
                mappedByteBuffer.get(addBytes);
                value = new String(bytes, 0, 4, StandardCharsets.UTF_8) + new String(addBytes, StandardCharsets.UTF_8);
            }
            else if (bytes[0] == '-') { // -n.n
                value = new String(bytes, 0, 4, StandardCharsets.UTF_8);
            }
            else { // nn.n
                value = new String(bytes, 0, 4, StandardCharsets.UTF_8);
            }
            mappedByteBuffer.get();
        }

        // System.out.println("TEMPERATURE: " + value);

        return Float.parseFloat(value);
    }
}
