package uk.co.onsdigital.job.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import uk.co.onsdigital.job.exception.ServiceUnavailableException;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.FilterRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
    }

    /**
     * Submits a request to the CSV filter component to produce the given output formats.
     *
     * @param dataSetS3Url  the S3 URL of the dataset to filter.
     * @param files         the files to create.
     * @param filters       the set of dimension filters to apply.
     */
    public void submitFilterRequest(final String dataSetS3Url, final Map<FileFormat, FileStatus> files,
                                    final Map<String, ? extends Set<String>> filters) {

        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("No files specified");
        }

        for (FileStatus file : files.values()) {
            if (file.isComplete()) {
                log.debug("Skipping file - already exists: {}", file);
                continue;
            }

            final FilterRequest filterRequest = FilterRequest.builder()
                    .inputUrl(dataSetS3Url)
                    .outputUrl("s3://" + outputS3Bucket + "/" + file.getName())
                    .dimensions(filters)
                    .build();

            try {
                final String json = jsonObjectMapper.writeValueAsString(filterRequest);
                log.debug("Sending filter request to Kafka: {}", json);
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, json));
            } catch (IOException e) {
                log.error("Unable to send message to Kafka: {}", e);
                throw new ServiceUnavailableException(e.getMessage());
            }
        }
    }
}
