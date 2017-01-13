package uk.co.onsdigital.job.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.job.exception.ServiceUnavailableException;
import uk.co.onsdigital.job.model.DimensionFilter;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.FilterRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Client for requesting that an input dataset is filtered to create one or more output files.
 */
@Service
public class FilterServiceClient {
    private static final Logger log = LoggerFactory.getLogger(FilterServiceClient.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper jsonObjectMapper;
    private final String outputS3Bucket;
    private final String kafkaTopic;

    private final Base64.Encoder filenameEncoder;

    @Autowired
    FilterServiceClient(final KafkaProducer<String, String> kafkaProducer,
                        final ObjectMapper jsonObjectMapper,
                        final @Value("${output.s3.bucket}") String outputS3Bucket,
                        final @Value("${kafka.topic}") String kafkaTopic) {
        log.info("Starting FilterServiceClient. kafka.topic={}, output.s3.bucket={}", kafkaTopic, outputS3Bucket);

        this.kafkaProducer = kafkaProducer;
        this.jsonObjectMapper = jsonObjectMapper;
        this.outputS3Bucket = outputS3Bucket;
        this.kafkaTopic = kafkaTopic;

        this.filenameEncoder = Base64.getUrlEncoder().withoutPadding();
    }

    /**
     * Submits a request to the CSV filter component to produce the given output formats.
     *
     * @param dataSet the dataset to filter.
     * @param filters the set of dimension filters to apply.
     * @param outputFormats the output formats to produce.
     * @return the initial status of each output file format.
     */
    public Map<FileFormat, FileStatus> submitFilterRequest(final DimensionalDataSet dataSet,
                                                           final List<DimensionFilter> filters,
                                                           final Set<FileFormat> outputFormats) {

        // Filter only supports CSV at the moment
        if (!outputFormats.equals(Collections.singleton(FileFormat.CSV))) {
            throw new IllegalArgumentException("Unsupported output formats: " + outputFormats);
        }

        final Map<FileFormat, FileStatus> statusMap = new EnumMap<>(FileFormat.class);
        // Transform the input dimensions format into the format expected by the filter
        final SortedMap<String, SortedSet<String>> dimensions = convertFilters(filters);

        final String filenameRoot = generateFileNameRoot(dimensions);

        for (FileFormat format : outputFormats) {
            final String filename = filenameRoot + format.getExtension();
            final FilterRequest filterRequest = FilterRequest.builder()
                    .inputUrl(dataSet.getS3URL())
                    .outputUrl("s3://" + outputS3Bucket + "/" + filename)
                    .dimensions(dimensions)
                    .build();

            try {
                final String json = jsonObjectMapper.writeValueAsString(filterRequest);
                log.debug("Sending filter request to Kafka: {}", json);
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, json));
            } catch (IOException e) {
                log.error("Unable to send message to Kafka: {}", e);
                throw new ServiceUnavailableException(e.getMessage());
            }

            final FileStatus status = new FileStatus(filename);
            statusMap.put(format, status);
        }

        log.info("Submitted jobs to CSV filter: {}", statusMap);
        return statusMap;
    }

    /**
     * Generates the root of the filename as the SHA-256 hash of the list of dimension filters in a predictable order.
     */
    private String generateFileNameRoot(SortedMap<String, SortedSet<String>> filters) {
        try {
            final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(filters.toString().getBytes(StandardCharsets.UTF_8));
            return filenameEncoder.encodeToString(sha256.digest());
        } catch (NoSuchAlgorithmException e) {
            // Should never happen as SHA-256 is one of the standard digest names.
            log.warn("Missing SHA-256 message digest - falling back on random uuid");
            return UUID.randomUUID().toString();
        }
    }

    /**
     * Convert input dimension filters into the format that the CSV Filter component expects. We use sorted maps and
     * sets here to provide a predictable order to {@link #generateFileNameRoot(SortedMap)}.
     *
     * @param filters the input filters.
     * @return the transformed filters.
     */
    private static SortedMap<String, SortedSet<String>> convertFilters(final List<DimensionFilter> filters) {
        final SortedMap<String, SortedSet<String>> result = new TreeMap<>();
        for (DimensionFilter filter : filters) {
            result.put(filter.getId(), new TreeSet<>(filter.getOptions()));
        }
        return result;
    }
}
