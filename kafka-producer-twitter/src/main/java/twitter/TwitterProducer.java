package twitter;

import com.google.common.collect.Lists;
import com.sun.org.apache.bcel.internal.generic.LineNumberGen;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import jdk.nashorn.internal.codegen.CompilerConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.opto.Block;

import java.net.Inet4Address;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "70O8IEJdnhFt4JO13b9AqXLc6";
    String consumerSecret = "uxQt8LxwWTMrNWoJMpTdG4sSVl6tI0O8vPESkAZcRkcUmMQnNH";
    String token = "1298199842875990016-n9w2e0pGp5da42NuPZF3I5IR4jAplA";
    String secret = "H3t4ZEiW2l2p2D6uXFlsRIwviiMRBqXd5QguYXsrK5Lwy";

    List<String> terms = Lists.newArrayList("kafka", "bitcoin", "USA", "soccer", "politics");


    public TwitterProducer() {
    }

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    private void run() {
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);

        //create a twitter client

        Client client = createTwitterClient(msgQueue);


        //Attempt to establish a connection
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        //add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    //producer cannot produce to a topic that does not exist so will have to create one
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");


    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.


    }

    public KafkaProducer<String, String> createKafkaProducer() {


        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



        //create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        //kafka 2.0 > so we can keep this as 5; use 1 otherwise.

        //high throughout settings at the expense of a biut of latency and CPU usage

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //good overall compression alogorithm by google
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //wait 20 ms. introducing delay
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size;




        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;

    }

}



