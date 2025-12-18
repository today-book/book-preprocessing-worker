# Book Preprocessing Worker

## Module layout
- `common-kafka`: shared Kafka utilities (producer/consumer configs, generic JSON publisher).
- `bookpreprocessingworker` (root app): business logic for parsing book messages (JSON/raw string), domain model, and Kafka bindings wired via ports.

## Kafka topology
- Input (JSON): `app.kafka.input-topic` (default `book.raw.naver`) → `JsonBookKafkaListener` → `BookPreprocessingService` (`app.kafka.json-group-id` optional).
- Input (raw string row): `app.kafka.csv-input-topic` (default `book.raw.csv`) → `CsvBookKafkaListener` → `BookPreprocessingService` (`app.kafka.csv-group-id` optional).
- Output: `app.kafka.output-topic` (default `book.parsed`) via `KafkaBookMessagePublisher`.

## Run tests
- Unit/Listener tests: `./gradlew test --tests '*BookPreprocessingServiceTest' --tests '*JsonBookKafkaListenerTest' --tests '*CsvBookKafkaListenerTest'`
- Full test suite: `./gradlew test`

## Notes
- Raw payloads are a single quoted row string (topic name contains "csv" but the payload is just a string). Description is taken from the image-adjacent columns, slug columns are ignored.
- Topic names and group IDs are property-driven; set `APP_KAFKA_CSV_INPUT_TOPIC` etc. per environment.
- Group IDs: `APP_KAFKA_JSON_GROUP_ID`, `APP_KAFKA_CSV_GROUP_ID` can override the defaults.
