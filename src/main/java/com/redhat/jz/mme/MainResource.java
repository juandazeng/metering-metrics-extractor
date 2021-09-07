package com.redhat.jz.mme;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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
        System.out.println("Running main method");
        Quarkus.run(MmeApp.class, args); 
    }

    public static class MmeApp implements QuarkusApplication {
        private static final Logger LOG = Logger.getLogger(MmeApp.class.getName());
        private static final String CATEGORY_PREFIX = "hive/metering.db/datasource_operator_metering_pod_";
        private static final String HEADER = "amount\tdate\ttime\ttimeprecision\tnamespace\tpod\tnode";

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
    
        @Override
        public int run(String... args) throws Exception {
            String dir = workingDirectory;
            if (workingDirectory == null || "".equals(workingDirectory) || ".".equals(workingDirectory)) {
                dir = ".";
            }
            workingDirectory = Paths.get(dir).toAbsolutePath().normalize().toString();
            debug(workingDirectory);
            Calendar startingDate = new GregorianCalendar(TimeZone.getTimeZone(targetTimeZone));
            startingDate.set(Integer.parseInt(startingyyyy), Integer.parseInt(startingMM)-1, 1, 0, 0, 0);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
            //startingDate.add(Calendar.HOUR_OF_DAY, -1);
            debug(startingDate.get(Calendar.YEAR) + "-" + (startingDate.get(Calendar.MONTH)+1) + "-" + startingDate.get(Calendar.DATE) + "-" + startingDate.get(Calendar.HOUR_OF_DAY) + ":" + startingDate.getTimeZone().getDisplayName());
            startingDate.setTimeZone(TimeZone.getTimeZone("UTC"));
            //debug(sdf.format(startingDate.getTimeZone()));
            debug(startingDate.get(Calendar.YEAR) + "-" + (startingDate.get(Calendar.MONTH)+1) + "-" + startingDate.get(Calendar.DATE) + "-" + startingDate.get(Calendar.HOUR_OF_DAY) + ":" + startingDate.getTimeZone().getDisplayName());
            
            downloadFromS3();

            Quarkus.waitForExit();
            return 0;
        }


        public void downloadFromS3() {
            // Calculate the target starting date yyyy-MM-01 (GMT+8)
            Calendar startingDate = new GregorianCalendar(TimeZone.getTimeZone(targetTimeZone));
            startingDate.set(Integer.parseInt(startingyyyy), Integer.parseInt(startingMM)-1, 1, 0, 0, 0);
            debug(startingDate.get(Calendar.YEAR) + "-" + (startingDate.get(Calendar.MONTH)+1) + "-" + startingDate.get(Calendar.DATE) + "-" + startingDate.get(Calendar.HOUR_OF_DAY) + ":" + startingDate.getTimeZone().getDisplayName());

            // Since the data is stored in UTC, we need to convert it to UTC timezone
            startingDate.setTimeZone(TimeZone.getTimeZone("UTC"));
            int startingYear = startingDate.get(Calendar.YEAR);
            int startingMonth = (startingDate.get(Calendar.MONTH)+1);
            int startingDay = startingDate.get(Calendar.DATE);
            debug(startingDate.get(Calendar.YEAR) + "-" + (startingDate.get(Calendar.MONTH)+1) + "-" + startingDate.get(Calendar.DATE) + "-" + startingDate.get(Calendar.HOUR_OF_DAY) + ":" + startingDate.getTimeZone().getDisplayName());

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
                String categoryName = categoryPrefixKey.substring(CATEGORY_PREFIX.length(), categoryPrefixKey.length() - 2);
                debug("Processing category:" + categoryPrefixKey + "\n---------------------\n");

                // This will store aggregated metrics data like this:
                // usage_cpu_cores - Namespace A:
                // -------------------------------
                // 2021-05-01 78.56598
                // 2021-05-02 53.00000
                // 2021-05-03 82.05500
                // ....
                Map<String, Map<String, Double>> metricsByNamespace = new TreeMap<>();

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
                        debug("Processing date:" + categoryName + "-" + datePrefixKey.substring(datePrefixKey.length() - 11, datePrefixKey.length() - 1));
                        
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
                                    orcToMetricsMap(orcFileAbsolutePath, metricsByNamespace, startingYear, startingMonth);
            
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
                for (Entry<String, Map<String, Double>> entrySet : metricsByNamespace.entrySet()) {
                    String outputFileName = categoryName + "_" + entrySet.getKey() + ".txt";
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(workingDirectory, outputFileName)))) {
                        for (Entry<String, Double> metricsByDate : entrySet.getValue().entrySet()) {
                            writer.write(metricsByDate.getKey() + "\t" + metricsByDate.getValue());
                            writer.newLine();
                        }
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    
        private void orcToMetricsMap(String orcFileAbsolutePath, Map<String, Map<String, Double>> metricsByNamespace, int startingYear, int startingMonth) throws IOException {
            
            try (Reader reader = OrcFile.createReader(
                new org.apache.hadoop.fs.Path(orcFileAbsolutePath),
                OrcFile.readerOptions(new Configuration(false))
            )) {
                TypeDescription schema = reader.getSchema();
                List<String> fieldNames = schema.getFieldNames();
                
                try (RecordReader records = reader.rows(reader.options())) {
                    VectorizedRowBatch batch = schema.createRowBatch();
    
                    while (records.nextBatch(batch)) {
                        for(int row=0; row < batch.size; ++row) {
                            Double utilisation = null;
                            String namespace = null;
                            Calendar timestamp = null;

                            for (int col=0; col < fieldNames.size(); col++) {
                                ColumnVector columnVector = batch.cols[col];
                                if (columnVector instanceof DoubleColumnVector) {
                                    // Only process the first double field, which is the utilisation field
                                    if (utilisation == null) {
                                        utilisation = ((DoubleColumnVector) columnVector).vector[row];
                                    }

                                } else if (columnVector instanceof TimestampColumnVector) {
                                    // Only process the first timestamp, which is the timestamp we're interested in
                                    if (timestamp == null) {
                                        // This timestamp is in UTC
                                        long time = ((TimestampColumnVector) columnVector).time[row];
                                        Calendar sourceTimestamp = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                                        sourceTimestamp.setTimeInMillis(time);
                                        debug(sourceTimestamp.get(Calendar.YEAR) + "-" + (sourceTimestamp.get(Calendar.MONTH)+1) + "-" + sourceTimestamp.get(Calendar.DATE) + "-" + sourceTimestamp.get(Calendar.HOUR_OF_DAY) + ":" + sourceTimestamp.getTimeZone().getDisplayName());

                                        // Convert it to our target timezone
                                        sourceTimestamp.setTimeZone(TimeZone.getTimeZone(targetTimeZone));
                                        timestamp = sourceTimestamp;
                                        debug(timestamp.get(Calendar.YEAR) + "-" + (timestamp.get(Calendar.MONTH)+1) + "-" + timestamp.get(Calendar.DATE) + "-" + timestamp.get(Calendar.HOUR_OF_DAY) + ":" + timestamp.getTimeZone().getDisplayName());
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

                            // Only store the data if it's within our desired date range
                            int currentYear = timestamp.get(Calendar.YEAR);
                            int currentMonth = (timestamp.get(Calendar.MONTH)+1);

                            if ((currentYear*12 + currentMonth) >=
                                (startingYear*12 + startingMonth)) {
                                // Store the data in the Map
                                Map<String, Double> metricsByDate = metricsByNamespace.get(namespace);
                                if (metricsByDate == null) {
                                    metricsByDate = new TreeMap<>();
                                    metricsByNamespace.put(namespace, metricsByDate);
                                }
                                String yyyyMMdd = timestamp.get(Calendar.YEAR) + "-" + (timestamp.get(Calendar.MONTH)+1) + "-" + timestamp.get(Calendar.DATE);
                                Double metrics = metricsByDate.get(yyyyMMdd);
                                if (metrics == null) {
                                    metricsByDate.put(yyyyMMdd, utilisation);
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