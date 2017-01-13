package uk.co.onsdigital.job;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.orm.jpa.JpaBaseConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.vendor.AbstractJpaVendorAdapter;
import org.springframework.orm.jpa.vendor.EclipseLinkJpaVendorAdapter;
import org.springframework.transaction.jta.JtaTransactionManager;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.job.model.Job;
import uk.co.onsdigital.job.repository.DataSetRepository;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Spring boot application configuration.
 */
@SpringBootApplication
@EntityScan(basePackageClasses = { DimensionalDataSet.class, Job.class })
@EnableJpaRepositories(basePackageClasses = DataSetRepository.class)
public class Application extends JpaBaseConfiguration {
    private static final Logger log = LoggerFactory.getLogger(Application.class);

    protected Application(DataSource dataSource, JpaProperties properties,
                            ObjectProvider<JtaTransactionManager> jtaTransactionManagerProvider) {
        super(dataSource, properties, jtaTransactionManagerProvider);
    }


    public static void main(String... args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    KafkaProducer<String, String> getKafkaProducer(@Value("${kafka.server}") String bootstrapServers) {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());

        log.info("Starting Kafka Producer: {}", kafkaProperties);
        return new KafkaProducer<>(kafkaProperties);
    }

    @Bean
    AmazonS3Client getAmazonS3Client() {
        final AmazonS3Client client = new AmazonS3Client();
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
        return client;
    }

    @Override
    protected AbstractJpaVendorAdapter createJpaVendorAdapter() {
        return new EclipseLinkJpaVendorAdapter();
    }

    @Override
    protected Map<String, Object> getVendorProperties() {
        return new HashMap<>();
    }
}
