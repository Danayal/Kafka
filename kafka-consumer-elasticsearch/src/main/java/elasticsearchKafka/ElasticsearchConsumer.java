package elasticsearchKafka;
//
//import com.sun.org.slf4j.internal.Logger;
//import com.sun.org.slf4j.internal.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.impl.StaticLoggerBinder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticsearchConsumer {



    public static RestHighLevelClient createClient() {

        String hostname = "localhost";

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "kafka-demo-elasticsearch";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Logger logger = LoggerFactory.getLogger(Class.forName(ElasticsearchConsumer.class.getName()));


        RestHighLevelClient client = createClient();



        String jsonString = "{ \"foo\": \"bar\"}";




        //IndexRequest indexRequest = new IndexRequest("tweets");

//        request.source(jsonString, XContentType.JSON);

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        //poll for new data
        while (true) {


            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();

            logger.info("Recieved " + recordCount + " records");


            BulkRequest bulkRequest = new BulkRequest();


            for (ConsumerRecord<String, String> record : records) {

                //2 strategies for making IDs
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                //twitter feed specific id
                String id = extractIDFromTweet(record.value());

                //insert data into elasticsearch
                IndexRequest indexRequest = new IndexRequest("twitter")
                        .id(id)
                        .source(record.value(), XContentType.JSON);


                bulkRequest.add(indexRequest); // add to our bulk request (takes no timne)
                logger.info(id);

            }

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets..");
                consumer.commitSync();
                logger.info("Offsets have been comitted");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

    }


//    JsonParser jsonParser = new JsonParser();
    private static String extractIDFromTweet(String tweetJSON) {

        //jsonParser.parse(tweetJSON).getAsJsonObject().get("id_str").getAsString();

        String jsonObject = JsonParser.parseString(tweetJSON).getAsJsonObject().get("id_str").getAsString();

        return jsonObject;


        //gson library


    }

}
