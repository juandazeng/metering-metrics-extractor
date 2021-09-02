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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    
        @Override
        public int run(String... args) throws Exception {
            String dir = workingDirectory;
            if (workingDirectory == null || "".equals(workingDirectory) || ".".equals(workingDirectory)) {
                dir = ".";
            }
            workingDirectory = Paths.get(dir).toAbsolutePath().normalize().toString();
            debug(workingDirectory);
            downloadFromS3();
            Quarkus.waitForExit();
            return 0;
        }


        public void downloadFromS3() {
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

                // Get the date folders in this folder. The result should look similar to this:
                // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-23/
                // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-24/
                // hive/metering.db/datasource_operator_metering_pod_limit_cpu_cores/dt=2021-06-25/
                ListObjectsV2Request dateListRequest = ListObjectsV2Request.builder().prefix(categoryPrefix.prefix() + "dt=").delimiter("/").bucket(bucketName).build();

                for (CommonPrefix datePrefix : s3.listObjectsV2(dateListRequest).commonPrefixes()) {
                    String datePrefixKey = datePrefix.prefix();

                    // Output file (1 file = 1 date)
                    String outputFileName = categoryName + "-" + datePrefixKey.substring(datePrefixKey.length() - 11, datePrefixKey.length() - 1) + ".txt";
                    debug("Processing file:" + outputFileName + "\n");
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(workingDirectory, outputFileName)))) {
                        // Write the heading
                        writer.write(HEADER);
                        writer.newLine();

                        // Get metrics files in this folder
                        ListObjectsV2Request metricsListRequest = ListObjectsV2Request.builder().prefix(datePrefixKey).bucket(bucketName).build();
                        
                        boolean done = false;
                        do {
                            
                            ListObjectsV2Response listResponse = s3.listObjectsV2(metricsListRequest);
                            for (S3Object s3o : listResponse.contents()) {
                                try {
                                    // Save the ORC data to a local file
                                    String orcFileAbsolutePath = saveOrcDataToFile(s3o.key());
            
                                    // Convert the ORC file into CSV (tab-separated)
                                    orcToCsv(orcFileAbsolutePath, writer);
            
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

                        // Probably not necessary since it's in try-with above
                        writer.flush();
                        writer.close();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    
        private void orcToCsv(String orcFileAbsolutePath, BufferedWriter writer) throws IOException {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'\t'HH:mm:ss.SSSZ");
            
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
                            StringBuilder sb = new StringBuilder();
                            for (int col=0; col < fieldNames.size(); col++) {
                                ColumnVector columnVector = batch.cols[col];
                                if (columnVector instanceof DoubleColumnVector) {
                                    sb.append(((DoubleColumnVector) columnVector).vector[row]);

                                } else if (columnVector instanceof TimestampColumnVector) {
                                    long time = ((TimestampColumnVector) columnVector).time[row];
                                    sb.append(sdf.format(new Date(time)));

                                } else if (columnVector instanceof MapColumnVector) {
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

                                    sb.append(labelsMap.get("namespace") + "\t");
                                    sb.append(labelsMap.get("pod") + "\t");
                                    sb.append(labelsMap.get("node"));

                                } else if (columnVector instanceof BytesColumnVector) {
                                    sb.append(((BytesColumnVector) columnVector).toString(row));

                                } else {
                                    throw new IOException("Unknown column type:" + columnVector.toString());
                                }

                                if (col+1 < fieldNames.size()) {
                                    sb.append("\t");
                                }
                            }
                            writer.write(sb.toString());
                            writer.newLine();

                            // For debugging, print just the last line
                            if (row+1 == batch.size) {
                                debug("Last line:" + sb.toString());
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