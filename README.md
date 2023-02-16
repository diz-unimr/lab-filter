# lab-filter
> Kafka consumer / producer that filters laboratory FHIR data

FHIR bundles are read from a Kafka input topic, filtered and send to an output topic. 

## Filters

There is currently only a single filter implemented, which parses bundles and removes all 
`Observation` resources from it which contain a `valueString` property.

In addition, references to those Observations are removed from the `DiagnosticReport` resource.
The whole bundle is discarded in case no Observation references are left in `DiagnosticReport.result`. 

## Configuration properties

| Name                             | Default                | Description                             |
|----------------------------------|------------------------|-----------------------------------------|
| `app.name`                       | lab-filter             | Kafka consumer group id                 |
| `log-level`                      | info                   | Log level (error,warn,info,debug,trace) |
| `kafka.bootstrap-servers`        | localhost:9092         | Kafka brokers                           |
| `kafka.security-protocol`        | ssl                    | Kafka communication protocol            |
| `kafka.input-topic`              |                        | Kafka topic to consume                  |
| `kafka.output-topic`             |                        | Kafka topic to send filtered bundles to |
| `kafka.ssl.ca-location`          | /app/cert/kafka-ca.pem | Kafka CA certificate location           |
| `kafka.ssl.certificate-location` | /app/cert/app-cert.pem | Client certificate location             |
| `kafka.ssl.key-location`         | /app/cert/app-key.pem  | Client  key location                    |
| `kafka.ssl.key-password`         | private-key-password   | Client key password                     |


### Environment variables

Override configuration properties by providing environment variables with their respective names. 
Upper case env variables are supported as well as underscores (`_`) instead of `.` and `-`. 