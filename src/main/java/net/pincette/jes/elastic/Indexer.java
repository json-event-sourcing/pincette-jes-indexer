package net.pincette.jes.elastic;

import static java.lang.String.valueOf;
import static java.lang.System.exit;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.parse;
import static java.util.logging.Logger.getLogger;
import static javax.json.Json.createObjectBuilder;
import static net.pincette.jes.elastic.Logging.log;
import static net.pincette.jes.util.Configuration.loadDefault;
import static net.pincette.jes.util.JsonFields.DELETED;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Streams.start;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetForever;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;
import static org.asynchttpclient.Dsl.asyncHttpClient;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.jes.util.Streams;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;

/**
 * This service connects Kafka topics to indices in Elasticsearch. The topics should use the
 * serialisation of JSON Event Sourcing. If an object contains the field "_id" it will be used as
 * the identifier in the index, otherwise a random UUID is used.
 *
 * @author Werner Donn\u00e9
 */
public class Indexer {
  private static final String ENVIRONMENT = "environment";
  private static final String KAFKA = "kafka";
  private static final String LOG_LEVEL = "logLevel";
  private static final String LOG_TOPIC = "logTopic";
  private static final String VERSION = "1.0.2";
  private static final AsyncHttpClient client = asyncHttpClient();

  private static void connect(
      final String topic,
      final String uri,
      final String authorizationHeader,
      final StreamsBuilder builder,
      final Logger logger) {
    final KStream<String, JsonObject> stream = builder.stream(topic);

    stream
        .filter((k, v) -> !v.getBoolean(DELETED, false))
        .mapValues(
            v ->
                sendForever(
                    () -> sendPutMessage(removeMetadata(v), createUri(v, uri), authorizationHeader),
                    logger));

    stream
        .filter((k, v) -> v.getBoolean(DELETED, false))
        .mapValues(
            v ->
                sendForever(
                    () -> sendDeleteMessage(createUri(v, uri), authorizationHeader), logger));
  }

  private static String createUri(final JsonObject json, final String uri) {
    return uri
        + Optional.ofNullable(json.getString(ID, null)).orElseGet(() -> randomUUID().toString());
  }

  private static String fullUri(final String uri, final String index) {
    return uri + (uri.endsWith("/") ? "" : "/") + index + "/_doc/";
  }

  private static Response logResponse(final Response response, final Logger logger) {
    if (response.getStatusCode() >= 400) {
      logger.log(
          SEVERE,
          "{0} {1}\n{2}",
          new Object[] {
            valueOf(response.getStatusCode()), response.getStatusText(), response.getResponseBody()
          });
    }

    return response;
  }

  public static void main(final String[] args) {
    final StreamsBuilder builder = new StreamsBuilder();
    final Config config = loadDefault();
    final String authorizationHeader = config.getString("elastic.authorizationHeader");
    final String environment = tryToGetSilent(() -> config.getString(ENVIRONMENT)).orElse("dev");
    final Level logLevel = parse(tryToGetSilent(() -> config.getString(LOG_LEVEL)).orElse("INFO"));
    final String logTopic =
        tryToGetSilent(() -> config.getString(LOG_TOPIC)).orElse("log-" + environment);
    final Logger logger = getLogger("pincette-jes-indexer");
    final String uri = config.getString("elastic.uri");

    logger.setLevel(logLevel);

    config
        .getConfig("indices")
        .entrySet()
        .forEach(
            e ->
                connect(
                    e.getKey(),
                    fullUri(uri, e.getValue().unwrapped().toString()),
                    authorizationHeader,
                    builder,
                    logger));

    tryToDoWithRethrow(
        () ->
            createReliableProducer(
                fromConfig(config, KAFKA), new StringSerializer(), new JsonSerializer()),
        producer -> {
          final Topology topology = builder.build();

          log(logger, VERSION, environment, producer, logTopic);
          logger.log(INFO, "Topology:\n\n {0}", topology.describe());

          if (!start(topology, Streams.fromConfig(config, KAFKA))) {
            exit(1);
          }
        });
  }

  private static JsonObject removeMetadata(final JsonObject message) {
    return createObjectBuilder(message).remove(ID).remove(TYPE).remove(TIMESTAMP).build();
  }

  private static BoundRequestBuilder request(
      final String method, final String uri, final String authorizationHeader) {
    return client
        .prepare(method, uri)
        .setHeader("Authorization", authorizationHeader)
        .setHeader("Content-Type", "application/json");
  }

  private static CompletionStage<Response> send(final BoundRequestBuilder request) {
    return request.execute().toCompletableFuture();
  }

  private static CompletionStage<Response> sendDeleteMessage(
      final String uri, final String authorizationHeader) {
    return send(request("DELETE", uri, authorizationHeader));
  }

  private static boolean sendForever(
      final Supplier<CompletionStage<Response>> send, final Logger logger) {
    return tryToGetRethrow(
            () ->
                tryToGetForever(
                        () ->
                            send.get()
                                .thenApply(response -> logResponse(response, logger))
                                .thenApply(response -> must(response, r -> r.getStatusCode() < 400))
                                .thenApply(response -> true),
                        Duration.ofSeconds(5))
                    .toCompletableFuture()
                    .get())
        .orElse(false);
  }

  private static CompletionStage<Response> sendPutMessage(
      final JsonObject json, final String uri, final String authorizationHeader) {
    return send(request("PUT", uri, authorizationHeader).setBody(string(json)));
  }
}
