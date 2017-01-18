package uk.co.onsdigital.job.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class FilterServiceClientTest {
    private static final String OUTPUT_BUCKET = "test-bucket";
    private static final String KAFKA_TOPIC = "test-topic";
    private static final String INPUT_S3_URL = "s3://test/test.csv";
    private static final Pattern OUTPUT_URL_PATTERN = Pattern.compile("^s3://test-bucket/(.*)$");

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private KafkaProducer<String, String> mockKafkaProducer;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> recordArgumentCaptor;

    private FilterServiceClient filterServiceClient;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
        filterServiceClient = new FilterServiceClient(mockKafkaProducer, objectMapper, OUTPUT_BUCKET, KAFKA_TOPIC);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldRejectEmptyOutputFormats() {
        filterServiceClient.submitFilterRequest(INPUT_S3_URL, Collections.emptyMap(),
                Collections.singletonMap("test", Collections.singleton("a")));
    }

    @Test
    public void shouldSendJobRequestsToKafkaTopic() throws Exception {
        // Given
        Map<String, Set<String>> filters = ImmutableMap.<String, Set<String>>builder()
                .put("first", ImmutableSortedSet.of("a", "b"))
                .put("second", ImmutableSortedSet.of("c", "d"))
                .build();
        Map<FileFormat, FileStatus> files = Collections.singletonMap(FileFormat.CSV, new FileStatus("test.csv"));

        // When
        filterServiceClient.submitFilterRequest(INPUT_S3_URL, files, filters);

        // Then
        verify(mockKafkaProducer).send(recordArgumentCaptor.capture());
        assertThat(recordArgumentCaptor.getValue().topic()).isEqualTo(KAFKA_TOPIC);
        final Map<String, Object> request = objectMapper.readValue(recordArgumentCaptor.getValue().value(), Map.class);
        assertThat(request)
                .containsEntry("inputUrl", INPUT_S3_URL)
                .containsKeys("outputUrl", "dimensions");
        assertThat((Map) request.get("dimensions"))
                .containsEntry("first", Arrays.asList("a", "b"))
                .containsEntry("second", Arrays.asList("c", "d"));
        assertThat(request.get("outputUrl")).asString().matches(OUTPUT_URL_PATTERN);

    }
}