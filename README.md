# Book Preprocessing Worker

## Module layout
- `common-kafka`: shared Kafka utilities (producer/consumer configs, generic JSON publisher).
- `bookpreprocessingworker` (root app): business logic for parsing book messages (JSON/CSV), domain model, and Kafka bindings wired via ports.

## Kafka topology
- Input (JSON): `app.kafka.input-topic` (default `book.raw`) → `JsonBookKafkaListener` → `BookPreprocessingService`.
- Input (CSV string row): `app.kafka.csv-input-topic` (default `csv-book.raw`) → `CsvBookKafkaListener` → `BookPreprocessingService`.
- Output: `app.kafka.output-topic` (default `book.parsed`) via `KafkaBookMessagePublisher`.

## Run tests
- Unit/Listener tests: `./gradlew test --tests '*BookPreprocessingServiceTest' --tests '*JsonBookKafkaListenerTest' --tests '*CsvBookKafkaListenerTest'`
- Full test suite: `./gradlew test`

## Notes
- CSV payloads are a single quoted row; description is taken from the image-adjacent columns, slug columns are ignored.
- Topic names and group IDs are property-driven; set `APP_KAFKA_CSV_INPUT_TOPIC` etc. per environment.
