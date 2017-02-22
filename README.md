# Job Creator API

REST API for creating and checking the status of file generation jobs.

## Build and run

```bash
mvn clean package
java -jar target/*.jar

# or:
mvn spring-boot:run
```

## Configuration

The following environment variables can be configured:

| Environment variable  | Default                                    | Description
| --------------------- | ------------------------------------------ | -----------
| SERVER_PORT           | 20100                                      | The port to bind to
| OUTPUT_S3_BUCKET      | dp-dd-csv-filter                           | S3 bucket to output files to
| DOWNLOAD_URL_TEMPLATE | https://www.ons.gov.uk/download/{filename} | URL template to use when creating download links
| DB_URL                | jdbc:postgresql://localhost:5432/data_discovery | JDBC URL for metadata DB
| DB_USER               | data_discovery                             | Database user
| DB_PASSWORD           | password                                   | Database password
| DB_DRIVER             | org.postgresql.Driver                      | JDBC driver
| KAFKA_SERVER          | 127.0.0.1:9092                             | Kafka bootstrap server address
| KAFKA_TOPIC           | filter-request                             | Kafka topic to send filter requests to.

## API

As per the stub, this offers two API calls:

### Create Job

POST to /job with payload:

```json
{ 
  "id" : "the dataset uuid",
  "dimensions" : [
      { "id": "NACE", "options": ["blah", "whatever"] },
      { "id": "Sex", "options": ["Male"] }
  ],
  "fileFormats" : ["CSV"]
}
```

You will receive back a response like:

```json
{
  "id": "3fdb1aed-123f-4258-b76b-5620190c7524",
  "status": "Pending",
  "files": [
    {
      "name": "P_F5yHzPfwhiUnZzF498M9qX_ODpr_0O7DBogi96n4k.csv",
      "status": "Pending"
    }
  ]
}
```

### Check job status

Take the `id` from the response you got when creating the job and perform a GET on `/job/{id}` to get a response like:

```json
{
  "id": "3fdb1aed-123f-4258-b76b-5620190c7524",
  "status": "Complete",
  "files": [
    {
      "name": "P_F5yHzPfwhiUnZzF498M9qX_ODpr_0O7DBogi96n4k.csv",
      "status": "Complete",
      "url": "https://foo.com/download/P_F5yHzPfwhiUnZzF498M9qX_ODpr_0O7DBogi96n4k.csv"
    }
  ]
}
```