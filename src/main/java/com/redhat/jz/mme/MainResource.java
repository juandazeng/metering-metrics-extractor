package com.redhat.jz.mme;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

// IMPORTANT: Configuration:
// 1. Please set these 2 environment variables to their proper values:
//    AWS_ACCESS_KEY_ID
//    AWS_SECRET_ACCESS_KEY
// 2. In the application.properties, set bucket.name, quarkus.s3.aws.region, quarkus.s3.aws.credentials.type=default, and working.directory (where the target files will be stored)

@QuarkusMain
public class MainResource {
    public static void main(String ... args) {
        Quarkus.run(MmeApp.class, args); 
    }

    public static class MmeApp implements QuarkusApplication {
        private static final Logger LOG = Logger.getLogger(MmeApp.class.getName());
        private static final String CATEGORY_PREFIX = "hive/metering.db/datasource_operator_metering_pod_";
        // private static final String HEADER = "amount\tdate\ttime\ttimeprecision\tnamespace\tpod\tnode";

        @Inject
        S3Client s3;
    
        @ConfigProperty(name = "bucket.name")
        String bucketName;
    
        @ConfigProperty(name = "working.directory")
        String workingDirectory;
    
        @ConfigProperty(name = "starting.yyyy")
        String startingyyyy;
    
        @ConfigProperty(name = "starting.MM")
        String startingMM;
    
        @ConfigProperty(name = "target.timezone")
        String targetTimeZone;
    
        @ConfigProperty(name = "categories")
        String categories;
    
        @ConfigProperty(name = "namespace.suffix")
        String namespaceSuffix;
    
        @ConfigProperty(name = "output.delimiter")
        String outputDelimiter;
    
        @Override
        public int run(String... args) throws Exception {
            String dir = workingDirectory;
            if (workingDirectory == null || "".equals(workingDirectory) || ".".equals(workingDirectory)) {
                dir = ".";
            }
            workingDirectory = Paths.get(dir).toAbsolutePath().normalize().toString();
            
            LocalDateTime dateTimeWithoutTimeZone = LocalDateTime.of(Integer.parseInt(startingyyyy), Integer.parseInt(startingMM), 1, 0, 0, 0);
            ZonedDateTime targetDateTimeWithTimeZone = dateTimeWithoutTimeZone.atZone(ZoneId.of(targetTimeZone));
            ZonedDateTime sourceTimeWithTimeZone = targetDateTimeWithTimeZone.withZoneSameInstant(ZoneId.of("UTC"));
            debug(dateTimeWithoutTimeZone.toString());
            debug(targetDateTimeWithTimeZone.toString());
            debug(sourceTimeWithTimeZone.toString());
            debug(sourceTimeWithTimeZone.getYear() + "-" + sourceTimeWithTimeZone.getMonthValue() + "-" + sourceTimeWithTimeZone.getDayOfMonth() + " " + sourceTimeWithTimeZone.getHour() + ":" + sourceTimeWithTimeZone.getMinute());

            downloadFromS3();

            Quarkus.waitForExit();
            return 0;
        }

        public void downloadFromS3() {
            // Calculate the target starting date yyyy-MM-01 (GMT+8)
            int targetStartingYear = Integer.parseInt(startingyyyy);
            int targetStartingMonth = Integer.parseInt(startingMM);
            LocalDateTime dateTimeWithoutTimeZone = LocalDateTime.of(targetStartingYear, targetStartingMonth, 1, 0, 0, 0);
            ZonedDateTime targetDateTimeWithTimeZone = dateTimeWithoutTimeZone.atZone(ZoneId.of(targetTimeZone));
            // Since the data is stored in UTC, we need to convert it to UTC timezone
            ZonedDateTime sourceTimeWithTimeZone = targetDateTimeWithTimeZone.withZoneSameInstant(ZoneId.of("UTC"));

            int startingYear = sourceTimeWithTimeZone.getYear();
            int startingMonth = sourceTimeWithTimeZone.getMonthValue();
            int startingDay = sourceTimeWithTimeZone.getDayOfMonth();

            // Get a list of categories we are interested in
            String[] categoriesArray = categories.split(",");
            Set<String> categoriesSet = new HashSet<>();
            for (String category : categoriesArray) {
                categoriesSet.add(category.strip());
            }
            debug(categoriesSet.contains("usage_cpu") ? "yes" : "no");

            // Get the folders for pod utilisation. The result should look similar to this:
            // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/
            // hive/metering.db/datasource_operator_metering_pod_limit_memory_bytes/
            // hive/metering.db/datasource_operator_metering_pod_persistentvolumeclaim_request_info/
            // hive/metering.db/datasource_operator_metering_pod_request_cpu_cores/
            // hive/metering.db/datasource_operator_metering_pod_request_memory_bytes/
            // hive/metering.db/datasource_operator_metering_pod_usage_cpu_cores/
            // hive/metering.db/datasource_operator_metering_pod_usage_memory_bytes/
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder().prefix(CATEGORY_PREFIX).delimiter("/").bucket(bucketName).build();
            for (CommonPrefix categoryPrefix : s3.listObjectsV2(listRequest).commonPrefixes()) {
                String categoryPrefixKey = categoryPrefix.prefix();
                String categoryName = categoryPrefixKey.substring(CATEGORY_PREFIX.length(), categoryPrefixKey.length() - 1);
                if (categoriesSet.contains(categoryName)) {
                    debug("Processing category:" + categoryPrefixKey + "\n---------------------\n");

                    // This will store aggregated metrics data like this:
                    // usage_cpu_cores - Namespace A:
                    // -------------------------------
                    // 2021-05-01 78.56598
                    // 2021-05-02 53.00000
                    // 2021-05-03 82.05500
                    // ....
                    Map<String, Map<String, Double>> metricsByNamespace = new TreeMap<>();
                    debug("metricsByNamespace:" + metricsByNamespace.size());

                    // Get the date folders in this folder. The result should look similar to this:
                    // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-23/
                    // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-24/
                    // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-25/
                    ListObjectsV2Request dateListRequest = ListObjectsV2Request.builder().prefix(categoryPrefix.prefix() + "dt=").delimiter("/").bucket(bucketName).build();

                    for (CommonPrefix datePrefix : s3.listObjectsV2(dateListRequest).commonPrefixes()) {
                        String datePrefixKey = datePrefix.prefix();

                        // Only process data from startingDate onwards
                        int sourceYear = Integer.valueOf(datePrefixKey.substring(datePrefixKey.length() - 11, datePrefixKey.length() - 7));
                        int sourceMonth = Integer.valueOf(datePrefixKey.substring(datePrefixKey.length() - 6, datePrefixKey.length() - 4));
                        int sourceDay = Integer.valueOf(datePrefixKey.substring(datePrefixKey.length() - 3, datePrefixKey.length() - 1));
                        if ((sourceYear*365 + sourceMonth*30 + sourceDay) >=
                            (startingYear*365 + startingMonth*30 + startingDay)
                        ) {
                            // debug("Processing date:" + categoryName + "-" + datePrefixKey.substring(datePrefixKey.length() - 11, datePrefixKey.length() - 1));
                            
                            // Get metrics files in this folder
                            ListObjectsV2Request metricsListRequest = ListObjectsV2Request.builder().prefix(datePrefixKey).bucket(bucketName).build();
                            
                            boolean done = false;
                            do {
                                
                                ListObjectsV2Response listResponse = s3.listObjectsV2(metricsListRequest);
                                for (S3Object s3o : listResponse.contents()) {
                                    try {
                                        // Save the ORC data to a local file
                                        String orcFileAbsolutePath = saveOrcDataToFile(s3o.key());
                
                                        // Collect metrics data from the ORC file
                                        orcToMetricsMap(orcFileAbsolutePath, metricsByNamespace, targetStartingYear, targetStartingMonth);
                
                                        Files.delete(new File(orcFileAbsolutePath).toPath());
                                    } catch (IOException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }

                                // Next page/set of files
                                String nextContinuationToken = listResponse.nextContinuationToken();
                                if (nextContinuationToken != null) {
                                    metricsListRequest = metricsListRequest.toBuilder().continuationToken(nextContinuationToken).build();
                                } else {
                                    done = true;
                                }
                    
                            } while (!done);
                        }
                    }

                    // Save the metrics data to file
                    String outputFileName = categoryName + ".txt";
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(workingDirectory, outputFileName)))) {
                        for (Entry<String, Map<String, Double>> entrySet : metricsByNamespace.entrySet()) {
                            String namespace = entrySet.getKey();
                            for (Entry<String, Double> metricsByDate : entrySet.getValue().entrySet()) {
                                writer.write(namespace + outputDelimiter + metricsByDate.getKey() + outputDelimiter + metricsByDate.getValue());
                                writer.newLine();
                            }
                        }
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    debug("Skipping category:" + categoryPrefixKey + "\n---------------------\n");
                }
            } // for categories

            debug("Done.");
        }
    
        private void orcToMetricsMap(String orcFileAbsolutePath, Map<String, Map<String, Double>> metricsByNamespace, int targetStartingYear, int targetStartingMonth) throws IOException {
            
            try (Reader reader = OrcFile.createReader(
                new org.apache.hadoop.fs.Path(orcFileAbsolutePath),
                OrcFile.readerOptions(new Configuration(false))
            )) {
                TypeDescription schema = reader.getSchema();
                List<String> fieldNames = schema.getFieldNames();
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                
                try (RecordReader records = reader.rows(reader.options())) {
                    VectorizedRowBatch batch = schema.createRowBatch();
    
                    while (records.nextBatch(batch)) {
                        for(int row=0; row < batch.size; ++row) {
                            Double utilisation = null;
                            String namespace = null;
                            ZonedDateTime targetTimestamp = null;

                            for (int col=0; col < fieldNames.size(); col++) {
                                ColumnVector columnVector = batch.cols[col];
                                if (columnVector instanceof DoubleColumnVector) {
                                    // Only process the first double field, which is the utilisation field
                                    if (utilisation == null) {
                                        utilisation = ((DoubleColumnVector) columnVector).vector[row];
                                    }

                                } else if (columnVector instanceof TimestampColumnVector) {
                                    // Only process the first timestamp, which is the timestamp we're interested in
                                    if (targetTimestamp == null) {
                                        // This timestamp is in UTC
                                        long epochMilli = ((TimestampColumnVector) columnVector).time[row];
                                        Instant instant = Instant.ofEpochMilli(epochMilli);
                                        ZonedDateTime sourceTimestamp = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));

                                        // Convert it to targetTimeZone
                                        targetTimestamp = sourceTimestamp.withZoneSameInstant(ZoneId.of(targetTimeZone));
                                    }

                                } else if (columnVector instanceof MapColumnVector) {
                                    if (namespace == null) {
                                        MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
                                        BytesColumnVector keys = (BytesColumnVector) mapColumnVector.keys;
                                        BytesColumnVector values = (BytesColumnVector) mapColumnVector.values;

                                        // Extract data in the Map column type, into a HashMap
                                        Map<String, String> labelsMap = new HashMap<>();
                                        for(long i=mapColumnVector.offsets[row]; i < mapColumnVector.offsets[row] + mapColumnVector.lengths[row]; ++i) {
                                            StringBuilder keySb = new StringBuilder();
                                            StringBuilder valueSb = new StringBuilder();
                                            keys.stringifyValue(keySb, (int) i);
                                            values.stringifyValue(valueSb, (int) i);
                                            labelsMap.put(keySb.toString().replaceAll("\"", ""), valueSb.toString().replaceAll("\"", ""));
                                        }

                                        namespace = labelsMap.get("namespace");
                                    }

                                } else if (columnVector instanceof BytesColumnVector) {
                                    // We will ignore this string data for now

                                } else {
                                    // TODO: Handle unknown column type here
                                    // throw new IOException("Unknown column type:" + columnVector.toString());
                                }
                            }

                            // Only store the data if it's within our desired date range and the namespace suffix matches our namespace suffix
                            int currentYear = targetTimestamp.getYear();
                            int currentMonth = targetTimestamp.getMonthValue();

                            if (
                                ((currentYear*12 + currentMonth) >= (targetStartingYear*12 + targetStartingMonth)) &&
                                namespace.endsWith(namespaceSuffix)
                               ) {
                                // Store the data in the Map
                                Map<String, Double> metricsByDate = metricsByNamespace.get(namespace);
                                if (metricsByDate == null) {
                                    metricsByDate = new TreeMap<>();
                                    metricsByNamespace.put(namespace, metricsByDate);
                                }
                                String yyyyMMdd = dtf.format(targetTimestamp);
                                Double metrics = metricsByDate.get(yyyyMMdd);
                                if (metrics == null) {
                                    metricsByDate.put(yyyyMMdd, utilisation);
                                    debug(namespace + "." + yyyyMMdd + " - (namespaces, days):" + metricsByNamespace.size() + "." + metricsByDate.size());
                                } else {
                                    metricsByDate.put(yyyyMMdd, metrics + utilisation);
                                }
                            }
                        }
                    }
                }
            }  
        }
    
        private String saveOrcDataToFile(String keyName) throws IOException {
            File tempFile = File.createTempFile("orc-",".orc", new File(workingDirectory));
            GetObjectRequest objectRequest = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();
            try (InputStream inputStream = s3.getObjectAsBytes(objectRequest).asInputStream()) {
                Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            return tempFile.getAbsolutePath();
        }

        private static void debug(String message) {
            LOG.log(Level.FINE, message);
        }
    
    }

}