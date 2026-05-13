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

public class ProducerApp {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";

    public static void main(String[] args) {
        createTopics();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-producer-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

            // 1. Підписуємось на відповіді ДО відправки запиту
            consumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));
            consumer.poll(Duration.ofMillis(500)); // Робимо poll, щоб партиції були призначені

            // 2. Готуємо запит
            int start = 10;
            int finish = 100;
            String requestValue = start + "," + finish;
            String correlationId = UUID.randomUUID().toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(REQUEST_TOPIC, requestValue);
            record.headers().add("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8));

            // 3. Відправляємо запит
            producer.send(record);
            System.out.println("-> Запит надіслано: start=" + start + " finish=" + finish + " (id=" + correlationId + ")");

            // 4. Чекаємо на відповідь
            boolean responseReceived = false;
            long startTime = System.currentTimeMillis();
            long timeout = 3000 * 1000L; // 3000 секунд за умовою

            while (!responseReceived && (System.currentTimeMillis() - startTime) < timeout) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> rec : records) {
                    Header header = rec.headers().lastHeader("correlation-id");
                    if (header != null) {
                        String receivedCorrId = new String(header.value(), StandardCharsets.UTF_8);
                        if (correlationId.equals(receivedCorrId)) {
                            System.out.println("<- Отримано відповідь: avgSteps=" + rec.value());
                            responseReceived = true;
                            break;
                        }
                    }
                }
            }

            if (!responseReceived) {
                System.out.println("Час очікування відповіді минув!");
            }

            System.out.println("Готово. Контейнер живе.");
            // 5. Залишаємо сервіс живим
            while (true) {
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
            // Ігноруємо помилки, якщо топіки вже існують
            System.out.println("Топіки вже існують або сталася помилка створення: " + e.getMessage());
        }
    }
}