package uk.co.onsdigital.job.persistence;

import com.google.common.collect.ImmutableMap;
import org.eclipse.persistence.config.EntityManagerProperties;
import org.eclipse.persistence.config.PersistenceUnitProperties;
import org.eclipse.persistence.platform.database.H2Platform;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.persistence.*;
import java.util.Map;

/**
 * Base class for unit tests that interact with an in-memory database.
 */
public abstract class AbstractInMemoryDatabaseTests {

    private EntityManagerFactory emf;
    private EntityTransaction transaction;

    protected EntityManager entityManager;

    @BeforeClass
    public void setupPersistenceContext() {
        this.emf = getInMemoryEntityManagerFactory();
    }

    @BeforeMethod
    public void setup() {
        this.entityManager = emf.createEntityManager();
        this.transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterMethod
    public void rollback() {
        transaction.rollback();
    }

    private static EntityManagerFactory getInMemoryEntityManagerFactory() {
        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put(EntityManagerProperties.JDBC_URL, "jdbc:h2:mem:test")
                .put(EntityManagerProperties.JDBC_USER, "SA")
                .put(EntityManagerProperties.JDBC_PASSWORD, "")
                .put(EntityManagerProperties.JDBC_DRIVER, "org.h2.Driver")
                .put(PersistenceUnitProperties.DDL_GENERATION, PersistenceUnitProperties.DROP_AND_CREATE)
                .put(PersistenceUnitProperties.DDL_GENERATION_MODE, PersistenceUnitProperties.DDL_DATABASE_GENERATION)
                .put(PersistenceUnitProperties.TARGET_DATABASE, H2Platform.class.getName())
                .build();

        return Persistence.createEntityManagerFactory("data_discovery", props);
    }

}
