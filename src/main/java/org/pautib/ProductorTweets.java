package org.pautib;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class ProductorTweets {

    static final String TOPIC_NAME = "rawtweets";

    public static void main(String[] args) {

        String apiKey = args[0];
        String apiSecret = args[1];
        String tokenValue = args[2];
        String tokenSecret = args[2];

        final TwitterStream twitterStream = getTwitterStreamConfigured(apiKey, apiSecret, tokenValue, tokenSecret);
        final KafkaProducer<String, String> producer = getKafkaProducerConfigured();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                HashtagEntity[] hashtags = status.getHashtagEntities();
                if (hashtags.length > 0) {
                    String value = TwitterObjectFactory.getRawJSON(status);
                    String lang = status.getLang();
                    producer.send(new ProducerRecord<>(ProductorTweets.TOPIC_NAME, lang, value));
                }
            }
            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override
            public void onTrackLimitationNotice(int i) {}
            @Override
            public void onScrubGeo(long l, long l1) {}
            @Override
            public void onStallWarning(StallWarning stallWarning) {}
            @Override
            public void onException(Exception e) {}
        };

        twitterStream.addListener(listener);
        twitterStream.sample();

        producer.close();
    }

    private static KafkaProducer<String, String> getKafkaProducerConfigured() {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static TwitterStream getTwitterStreamConfigured(String apiKey, String apiSecret, String tokenValue, String tokenSecret) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthAccessToken(tokenValue);
        cb.setOAuthAccessTokenSecret(tokenSecret);
        cb.setOAuthConsumerKey(apiKey);
        cb.setOAuthConsumerSecret(apiSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        final TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        return twitterStream;
    }

}
