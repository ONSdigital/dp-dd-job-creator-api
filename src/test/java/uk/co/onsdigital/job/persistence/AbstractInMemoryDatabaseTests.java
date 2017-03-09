package uk.co.onsdigital.job.persistence;

import com.google.common.collect.ImmutableMap;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.H2Dialect;
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
                .put(AvailableSettings.JPA_JDBC_URL, "jdbc:h2:mem:test")
                .put(AvailableSettings.JPA_JDBC_USER, "SA")
                .put(AvailableSettings.JPA_JDBC_PASSWORD, "")
                .put(AvailableSettings.JPA_JDBC_DRIVER, "org.h2.Driver")
                .put(AvailableSettings.HBM2DDL_AUTO, "create-drop")
                .put(AvailableSettings.DIALECT, H2Dialect.class.getName())
                .build();

        return Persistence.createEntityManagerFactory("data_discovery", props);
    }

}
