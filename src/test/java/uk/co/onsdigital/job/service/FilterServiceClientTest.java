package uk.co.onsdigital.job.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.job.model.DimensionFilter;
import uk.co.onsdigital.job.model.FileFormat;
import uk.co.onsdigital.job.model.FileStatus;
import uk.co.onsdigital.job.model.Status;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class FilterServiceClientTest {
    private static final String OUTPUT_BUCKET = "test-bucket";
    private static final String KAFKA_TOPIC = "test-topic";
    private static final Pattern OUTPUT_URL_PATTERN = Pattern.compile("^s3://([^/]+)/(.*)$");

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
        filterServiceClient.submitFilterRequest(new DimensionalDataSet(),
                singletonList(new DimensionFilter("test", singletonList("one"))), emptySet());
    }

    @Test
    public void shouldSendJobRequestsToKafkaTopic() throws Exception {
        // Given
        String inputUrl = "s3://test/foo.csv";
        DimensionalDataSet dataSet = new DimensionalDataSet();
        dataSet.setS3URL(inputUrl);
        List<DimensionFilter> filters = Arrays.asList(new DimensionFilter("first", Arrays.asList("one", "two")),
                new DimensionFilter("second", Arrays.asList("three", "four")));
        Set<FileFormat> outputFormats = singleton(FileFormat.CSV);

        // When
        Map<FileFormat, FileStatus> result = filterServiceClient.submitFilterRequest(dataSet, filters, outputFormats);

        // Then
        verify(mockKafkaProducer).send(recordArgumentCaptor.capture());
        assertThat(recordArgumentCaptor.getValue().topic()).isEqualTo(KAFKA_TOPIC);
        final Map<String, Object> request = objectMapper.readValue(recordArgumentCaptor.getValue().value(), Map.class);
        assertThat(request)
                .containsEntry("inputUrl", inputUrl)
                .containsKeys("outputUrl", "dimensions");
        assertThat((Map) request.get("dimensions"))
                .containsEntry("first", Arrays.asList("one", "two"))
                .containsEntry("second", Arrays.asList("four", "three")); // Will be sorted as a side-effect

        final Matcher matcher = OUTPUT_URL_PATTERN.matcher(request.get("outputUrl").toString());
        assertThat(matcher.matches()).isTrue();
        final String bucket = matcher.group(1);
        final String filename = matcher.group(2);
        assertThat(bucket).isEqualTo(OUTPUT_BUCKET);

        assertThat(result).containsEntry(FileFormat.CSV, new FileStatus(filename));
        assertThat(result.get(FileFormat.CSV))
                .hasFieldOrPropertyWithValue("status", Status.PENDING)
                .hasFieldOrPropertyWithValue("url", null);

    }
}