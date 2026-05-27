# Flatten Kafka Connect Structs to JSON

Sometimes, when using Kafka Connect, you may want to send requests as a payload to a sink system.
If the record is deserialized as AVRO, the connector may be not smart enough to serialize it to JSON before sending it to the Sink.
See [this issue](https://github.com/confluentinc/schema-registry/issues/1354) for more info.

This minimal SMT takes the whole Connect record (which may or may not be a struct) and uses Jackson to turn it the equivalent JSON string representation.
Note that all schema information about the record is destroyed in the process. The SMT currently does not affect the key of the record in any way.

## Usage Example

Suppose that you have a topic with records that are Avro-serialized, and you want to send each record to an API endpoint. The endpoint expects JSON.
If you try to send the record without any transform, then the server will receive a payload that looks like this:

```
Struct{foo=1,bar=2}
```

To obtain an output like `{"foo":1,"bar":2}`, we add the SMT to the Connector configuration:

```
{
  "connector.class": "io.lenses.streamreactor.connect.http.sink.HttpSinkConnector",
  "topics": "my-topic",
  "tasks.max": "1",
  "connect.http.method": "POST",
  "connect.http.endpoint": "http://rest:3000",
  "connect.http.request.content": "{{value}}",
  "transforms": "jsonify",
  "transforms.jsonify.type": "io.spoud.kafka.connect.ToJson",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schemas.enable": "false",
  "value.converter.schema.registry.url": "http://schema-registry:8080/apis/ccompat/v7"
}
```
