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
import uk.co.onsdigital.job.model.FileDto;
import uk.co.onsdigital.job.model.FilterRequest;
import uk.co.onsdigital.logging.RequestIdProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;

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
    private final RequestIdProvider requestIdProvider;


    @Autowired
    FilterServiceClient(final KafkaProducer<String, String> kafkaProducer,
                        final ObjectMapper jsonObjectMapper,
                        final RequestIdProvider requestIdProvider,
                        final @Value("${output.s3.bucket}") String outputS3Bucket,
                        final @Value("${kafka.topic}") String kafkaTopic) {
        log.info("Starting FilterServiceClient. kafka.topic={}, output.s3.bucket={}", kafkaTopic, outputS3Bucket);

        this.kafkaProducer = kafkaProducer;
        this.jsonObjectMapper = jsonObjectMapper;
        this.outputS3Bucket = outputS3Bucket;
        this.kafkaTopic = kafkaTopic;
        this.requestIdProvider = requestIdProvider;
    }

    /**
     * Submits a request to the CSV filter component to produce the given output formats.
     *
     * @param dataSetS3Url  the S3 URL of the dataset to filter.
     * @param files         the files to create.
     * @param filters       the set of dimension filters to apply.
     */
    public void submitFilterRequest(final String dataSetS3Url, final Map<FileFormat, FileDto> files,
                                    final Map<String, ? extends Set<String>> filters) {

        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("No files specified");
        }

        for (FileDto file : files.values()) {
            if (file.isComplete()) {
                log.debug("Skipping file - already exists: {}", file);
                continue;
            }

            if (file.isSubmitted()) {
                // Check to see how long ago the file was submitted. If more than one hour, then submit again. This will
                // also update the submittedAt time so that we wait another hour before submitting it again.
                final Instant oneHourAgo = now().minus(1, HOURS);
                if (file.getSubmittedAt().toInstant().isBefore(oneHourAgo)) {
                    log.warn("File was submitted more than 1 hour ago but has not been generated - resubmitting: {}", file);
                } else {
                    log.debug("Skipping file - has already been submitted recently: {}", file);
                    continue;
                }
            }

            final FilterRequest filterRequest = FilterRequest.builder()
                    .requestId(requestIdProvider.getId())
                    .inputUrl(dataSetS3Url)
                    .outputUrl("s3://" + outputS3Bucket + "/" + file.getName())
                    .dimensions(filters)
                    .build();

            try {
                final String json = jsonObjectMapper.writeValueAsString(filterRequest);
                log.debug("Sending filter request to Kafka: {}", json);
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, json));
                file.setSubmittedAt(new Date());
                log.debug("Request successfully queued: {}", file.getName());
            } catch (IOException e) {
                log.error("Unable to send message to Kafka: {}", e);
                throw new ServiceUnavailableException(e.getMessage());
            }
        }
    }
}
