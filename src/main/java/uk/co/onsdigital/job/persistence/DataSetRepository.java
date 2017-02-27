package uk.co.onsdigital.job.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import uk.co.onsdigital.discovery.model.*;
import uk.co.onsdigital.job.exception.NoSuchDataSetException;

import javax.persistence.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repository for looking up datasets.
 */
@Component
public class DataSetRepository {

    /** Query that returns all existing dimensions that match the requested dimensions for a dataset. */
    public static final String DIMENSION_VALUES_QUERY = "SELECT dim.dimension.name, dim.value FROM DimensionValue dim " +
            "WHERE dim.dimension.dataSet.id = :ddsId " +
            "AND dim.dimension.name IN :names " +
            "AND dim.value IN :values";
    public static final String DATASET_ID_PARAM = "ddsId";
    public static final String NAMES_PARAM = "names";
    public static final String VALUES_PARAM = "values";

    private final EntityManager entityManager;

    @Autowired
    public DataSetRepository(EntityManager entityManager) {
        this.entityManager = entityManager;
    }


    /**
     * Looks up the S3 URL for the given dataset.
     *
     * @param dataSetId the id of the dataset to lookup.
     * @return the S3 URL of that dataset.
     * @throws NoSuchDataSetException if the dataset does not exist.
     */
    public String findS3urlForDataSet(UUID dataSetId) {
        try {
            return entityManager.createNamedQuery(DimensionalDataSet.LOOKUP_S3_URL, String.class)
                    .setParameter(DimensionalDataSet.ID_PARAM, dataSetId)
                    .getSingleResult();
        } catch (NoResultException e) {
            throw new NoSuchDataSetException(dataSetId);
        }
    }

    /**
     * Finds all dimension/value combinations that match the requested values for the given dataset.
     * @param datasetId the id of the dataset.
     * @param requestedValues the requested dimension values.
     * @return the subset of requestedValues that actually exist in the dataset.
     */
    public SortedMap<String, SortedSet<String>> findMatchingDimensionValues(UUID datasetId, SortedMap<String, SortedSet<String>> requestedValues) {
        Query query = entityManager.createQuery(DIMENSION_VALUES_QUERY);
        query.setParameter(DATASET_ID_PARAM, datasetId);
        query.setParameter(NAMES_PARAM, requestedValues.keySet());
        query.setParameter(VALUES_PARAM, requestedValues.values().stream().flatMap(Set::stream).collect(Collectors.toList()));
        List<Object[]> resultList = query.getResultList();
        SortedMap<String, SortedSet<String>> filtered = new TreeMap<>();
        for (Object[] pair : resultList) {
            String key = (String) pair[0];
            String value = (String) pair[1];
            // ensure we haven't added values from dimension x into dimension y
            if (requestedValues.get(key).contains(value)) {
                Set<String> values = filtered.computeIfAbsent(key, v -> new TreeSet<>());
                values.add(value);
            }
        }
        return filtered;
    }


}
