package net.pincette.jes.elastic;

import static java.lang.System.exit;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.INFO;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Streams.start;
import static net.pincette.json.JsonUtil.string;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import com.typesafe.config.Config;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import net.pincette.jes.util.Streams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.asynchttpclient.AsyncHttpClient;

/**
 * This service connects Kafka topics to indices in Elasticsearch. The topics should use the
 * serialisation of JSON Event Sourcing. If an object contains the field "_id" it will be used as
 * the identifier in the index, otherwise a random UUID is used.
 *
 * @author Werner Donn\u00e9
 */
public class Indexer {
  private static final String KAFKA = "kafka";
  private static final AsyncHttpClient client = asyncHttpClient();

  private static void connect(
      final String topic,
      final String uri,
      final String authorizationHeader,
      final StreamsBuilder builder) {
    final KStream<String, JsonObject> stream = builder.stream(topic);

    stream.mapValues(v -> sendMessage(v, createUri(v, uri), authorizationHeader));
  }

  private static String createUri(final JsonObject json, final String uri) {
    return uri
        + Optional.ofNullable(json.getString(ID, null)).orElseGet(() -> randomUUID().toString());
  }

  private static String fullUri(final String uri, final String index) {
    return uri + (uri.endsWith("/") ? "" : "/") + index + "/_doc/";
  }

  public static void main(final String[] args) {
    final StreamsBuilder builder = new StreamsBuilder();
    final Config config = loadDefault();
    final String authorizationHeader = config.getString("elastic.authorizationHeader");
    final String uri = config.getString("elastic.uri");

    config
        .getConfig("indices")
        .entrySet()
        .forEach(
            e ->
                connect(
                    e.getKey(),
                    fullUri(uri, e.getValue().unwrapped().toString()),
                    authorizationHeader,
                    builder));

    final Topology topology = builder.build();

    getLogger("pincette-jes-indexer").log(INFO, "Topology:\n\n {0}", topology.describe());

    if (!start(topology, Streams.fromConfig(config, KAFKA))) {
      exit(1);
    }
  }

  private static CompletionStage<Boolean> sendMessage(
      final JsonObject json, final String uri, final String authorizationHeader) {
    return client
        .prepare("PUT", uri)
        .setHeader("Authorization", authorizationHeader)
        .setHeader("Content-Type", "application/json")
        .setBody(string(json))
        .execute()
        .toCompletableFuture()
        .thenApply(r -> r.getStatusCode() == 202);
  }
}
