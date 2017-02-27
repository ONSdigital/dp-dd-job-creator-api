package uk.co.onsdigital.job.persistence;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;

import java.util.*;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataSetRepositoryTest extends AbstractInMemoryDatabaseTests {

    private DataSetRepository dataSetRepository;

    @BeforeMethod
    public void setupRepository() {
        this.dataSetRepository = new DataSetRepository(entityManager);
    }

    @Test
    public void shouldReturnS3UrlForExistingDatasets() {
        // Given
        UUID id = UUID.randomUUID();
        String s3Url = "some s3 url";
        DimensionalDataSet dataSet = new DimensionalDataSet();
        dataSet.setId(id);
        dataSet.setS3URL(s3Url);
        entityManager.persist(dataSet);

        // When
        String result = dataSetRepository.findS3urlForDataSet(id);

        // Then
        assertThat(result).isEqualTo(s3Url);
    }

    @Test(expectedExceptions = NoSuchDataSetException.class)
    public void shouldThrowNoSuchDataSetExceptionIfNotFound() {
        dataSetRepository.findS3urlForDataSet(UUID.randomUUID());
    }

    // findMatchingDimensionValues

    @Test
    public void shouldReturnEmptyMapWhenNothingMatches() {
        SortedMap<String, SortedSet<String>> requestedValues = new TreeMap<>();
        requestedValues.put("invalid", new TreeSet(singleton("foo")));
        SortedMap<String, SortedSet<String>> result = dataSetRepository.findMatchingDimensionValues(UUID.randomUUID(), requestedValues);
        assertThat(result).isEmpty();
    }

    @Test
    public void shouldReturnSingleDimensionValue() {
        DimensionalDataSet dataset = new DimensionalDataSet();
        dataset.setId(UUID.randomUUID());
        entityManager.persist(dataset);

        String dimension1 = "d1";
        String valueA = "valueA";
        persistDimensionWithValues(dataset, dimension1, valueA);

        SortedMap<String, SortedSet<String>> requestedValues = new TreeMap<>();
        requestedValues.put(dimension1, new TreeSet<>(Arrays.asList(valueA, "foo")));
        requestedValues.put("invalid", new TreeSet<>(Arrays.asList(valueA, "foo")));
        SortedMap<String, SortedSet<String>> result = dataSetRepository.findMatchingDimensionValues(dataset.getId(), requestedValues);
        assertThat(result).containsOnlyKeys(dimension1);
        assertThat(result).containsValues(new TreeSet(singleton(valueA)));

    }

    @Test
    public void shouldReturnValuesForAppropriateDimensionOnly() {
        DimensionalDataSet dataset = new DimensionalDataSet();
        dataset.setId(UUID.randomUUID());
        entityManager.persist(dataset);

        String dimension1 = "d1";
        String valueA = "valueA";
        String valueB = "valueB";
        persistDimensionWithValues(dataset, dimension1, valueA, valueB);

        String dimension2 = "d2";
        persistDimensionWithValues(dataset, dimension2, valueA, valueB);

        SortedMap<String, SortedSet<String>> requestedValues = new TreeMap<>();
        requestedValues.put(dimension1, new TreeSet<>(Arrays.asList(valueA)));
        requestedValues.put(dimension2, new TreeSet<>(Arrays.asList(valueB, "foo")));
        SortedMap<String, SortedSet<String>> result = dataSetRepository.findMatchingDimensionValues(dataset.getId(), requestedValues);
        assertThat(result).containsOnlyKeys(dimension1, dimension2);
        assertThat(result.get(dimension1)).containsExactly(valueA);
        assertThat(result.get(dimension2)).containsExactly(valueB);

    }

    @Test
    public void shouldIgnoreEmptyDimensions() {
        DimensionalDataSet dataset = new DimensionalDataSet();
        dataset.setId(UUID.randomUUID());
        entityManager.persist(dataset);

        String dimension1 = "d1";
        String valueA = "valueA";
        String valueB = "valueB";
        persistDimensionWithValues(dataset, dimension1, valueA, valueB);

        String dimension2 = "d2";
        persistDimensionWithValues(dataset, dimension2, valueA, valueB);

        SortedMap<String, SortedSet<String>> requestedValues = new TreeMap<>();
        requestedValues.put(dimension1, new TreeSet<>(Arrays.asList(valueA)));
        requestedValues.put(dimension2, new TreeSet<>());
        SortedMap<String, SortedSet<String>> result = dataSetRepository.findMatchingDimensionValues(dataset.getId(), requestedValues);
        assertThat(result).containsOnlyKeys(dimension1);
        assertThat(result.get(dimension1)).containsExactly(valueA);
    }

    private void persistDimensionWithValues(DimensionalDataSet dataset, String dimension1, String... values) {
        Dimension d1 = new Dimension();
        d1.setDataSet(dataset);
        d1.setName(dimension1);
        d1.setType("foo");
        entityManager.persist(d1);
        for (String value : values) {
            DimensionValue dv = new DimensionValue();
            dv.setId(UUID.randomUUID());
            dv.setDimension(d1);
            dv.setValue(value);
            entityManager.persist(dv);
        }
    }
}