package app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class KafkaStreamsConfiguration {

    private KafkaProperties kafkaProperties;

	@Autowired
    public void setKafkaProperties(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(JsonDeserializer.DEFAULT_KEY_TYPE, String.class);
        props.put(JsonDeserializer.DEFAULT_VALUE_TYPE, Object.class);
        return new StreamsConfig(props);
    }
	
    @Bean
    public KStream<String, Map<String, Object>> kJoinedStream(StreamsBuilder builder) {

    	KStream<String, HashMap> stream1 = builder.stream("user-topic", Consumed.with(Serdes.String(),
                new JsonSerde<>(HashMap.class)));

    	KStream<String, HashMap> stream2 = builder.stream("message-topic", Consumed.with(Serdes.String(),
                new JsonSerde<>(HashMap.class)));

        KStream<String, HashMap> modifiedStream1 = stream1.selectKey((k, user) -> user.get("id").toString());

        KStream<String, HashMap> modifiedStream2 = stream2.selectKey((k, msg) -> msg.get("id").toString());


    	KStream<String, Map<String, Object>> joined = modifiedStream1.join(modifiedStream2,
                (t1, t2) -> {
    	    t1.putAll(t2); return t1;
    	    }, JoinWindows.of(Duration.ofSeconds(3).toMillis()));

    	joined.foreach((c, cc) -> {
    	    StringBuilder line = new StringBuilder();
    	    cc.entrySet().stream().forEach(ent -> {
                line.append(ent.getKey()).append(":").append(ent.getValue()).append(" ");
            });
    	    log.info(line.toString().trim());
        });

    	joined.to("usr-msg-topic");

        return joined;
    }

}
