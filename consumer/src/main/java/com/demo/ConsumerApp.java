package com.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class ConsumerApp {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";

    public static void main(String[] args) {
        createTopics();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-responder-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));
            System.out.println("Чекаю запитів у '" + REQUEST_TOPIC + "'...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> rec : records) {

                    // Парсимо запит
                    String[] parts = rec.value().split(",");
                    int start = Integer.parseInt(parts[0].trim());
                    int finish = Integer.parseInt(parts[1].trim());

                    System.out.println("<- Отримано запит: start=" + start + " finish=" + finish);

                    // Обчислюємо Колатца
                    long avgSteps = calculateCollatzAverage(start, finish);

                    // Дістаємо correlationId
                    Header header = rec.headers().lastHeader("correlation-id");
                    if (header != null) {
                        byte[] correlationIdBytes = header.value();

                        // Відправляємо відповідь
                        ProducerRecord<String, String> replyRecord = new ProducerRecord<>(RESPONSE_TOPIC, String.valueOf(avgSteps));
                        replyRecord.headers().add("correlation-id", correlationIdBytes);

                        producer.send(replyRecord);
                        System.out.println("-> Надіслано відповідь: avgSteps=" + avgSteps);
                    }
                }
            }
        }
    }

    private static long calculateCollatzAverage(int start, int finish) {
        if (start > finish) return 0;
        long totalSteps = 0;
        int count = finish - start + 1;

        for (int i = start; i <= finish; i++) {
            long n = i;
            long steps = 0;
            while (n > 1) {
                if (n % 2 == 0) {
                    n /= 2;
                } else {
                    n = 3 * n + 1;
                }
                steps++;
            }
            totalSteps += steps;
        }
        return totalSteps / count;
    }

    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.asList(
                    new NewTopic(REQUEST_TOPIC, 1, (short) 1),
                    new NewTopic(RESPONSE_TOPIC, 1, (short) 1)
            );
            adminClient.createTopics(topics).all().get();
        } catch (Exception e) {
            System.out.println("Топіки вже існують або сталася помилка створення: " + e.getMessage());
        }
    }
}