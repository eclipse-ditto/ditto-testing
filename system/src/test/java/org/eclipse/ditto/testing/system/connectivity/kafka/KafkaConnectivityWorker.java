/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.testing.system.connectivity.kafka;

import static java.util.Objects.requireNonNull;

import java.nio.channels.ClosedSelectorException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.Logger;

import org.apache.pekko.japi.Pair;

public final class KafkaConnectivityWorker extends
        AbstractConnectivityWorker<BlockingQueue<ConsumerRecord<String, byte[]>>, ConsumerRecord<String, byte[]>> {

    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final KafkaProducerFactory kafkaProducerFactory;
    private final TargetAddressBuilder targetAddressBuilder;
    private final SourceAddressBuilder sourceAddressBuilder;

    private final ReplyTargetAddressBuilder replyTargetAddressBuilder;
    private final Duration kafkaPollTimeout;
    private final Duration waitTimeout;
    private final List<Pair<ExecutorService, KafkaConsumer<?, ?>>> pollingConsumers;
    @Nullable private KafkaProducer<String, byte[]> kafkaProducer = null;

    KafkaConnectivityWorker(final Logger logger,
            final KafkaConsumerFactory kafkaConsumerFactory,
            final KafkaProducerFactory kafkaProducerFactory,
            final TargetAddressBuilder targetAddressBuilder,
            final SourceAddressBuilder sourceAddressBuilder,
            final ReplyTargetAddressBuilder replyTargetAddressBuilder,
            final Duration kafkaPollTimeout,
            final Duration waitTimeout) {
        super(logger);
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.kafkaProducerFactory = kafkaProducerFactory;
        this.targetAddressBuilder = targetAddressBuilder;
        this.sourceAddressBuilder = sourceAddressBuilder;
        this.replyTargetAddressBuilder = replyTargetAddressBuilder;
        this.kafkaPollTimeout = kafkaPollTimeout;
        this.waitTimeout = waitTimeout;
        pollingConsumers = new ArrayList<>();
    }

    public static void setupKafka(final String clientId, final String hostname, final int port,
            final String username, final String password,
            final Collection<String> topics, final Logger logger)
            throws InterruptedException, ExecutionException, TimeoutException {

        setupKafka(clientId, hostname, port, username, password, topics, logger, 10);
    }

    public static void setupKafka(final String clientId, final String hostname, final int port,
            final String username, final String password,
            final Collection<String> topics, final Logger logger, final int retries)
            throws InterruptedException, ExecutionException, TimeoutException {

        try (final AdminClient adminClient =
                AdminClient.create(getKafkaAdminClientProperties(clientId, hostname, port, username, password))) {

            final List<NewTopic> newTopics =
                    topics.stream().map(KafkaConnectivityWorker::newKafkaTopic).collect(Collectors.toList());

            logger.info("Creating topics now ...");
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            try {
                createTopicsResult.all().get(10, TimeUnit.SECONDS);
                logger.info("Successfully created topics: {}", topics);
            } catch (final Exception e) {
                logger.info("Topics <{}> could not be created: {}", topics, e.getMessage());
            }

            if (logger.isDebugEnabled()) {
                final Set<String> kafkaTopics = listKafkaTopics(adminClient);
                logger.debug("Kafka topics: {}", kafkaTopics);
            }
        } catch (final KafkaException e) {
            final Duration retryDelay = Duration.ofSeconds(1L);
            final int remainingAttempts = retries - 1;
            logger.error("Failed to setup kafka, trying again in {}. Remaining attempts: {}", retryDelay,
                    remainingAttempts, e);
            if (remainingAttempts > 0) {
                Thread.sleep(retryDelay.toMillis());
                setupKafka(clientId, hostname, port, username, password, topics, logger, remainingAttempts);
            }
        }
    }

    /**
     * Stop all polling consumers from polling and return them.
     *
     * @return the consumers stopped from polling.
     */
    public List<KafkaConsumer<?, ?>> stopPolling() throws Exception {
        final List<KafkaConsumer<?, ?>> stoppedConsumers =
                pollingConsumers.stream().map(Pair::second).collect(Collectors.toList());
        final List<ExecutorService> executors =
                pollingConsumers.stream().map(Pair::first).collect(Collectors.toList());
        for (final KafkaConsumer<?, ?> consumer : stoppedConsumers) {
            // interrupt select call to have consumer stop polling
            try {
                consumer.wakeup();
            } catch (final ClosedSelectorException e) {
                // Consumer already closed, hence not polling. Everything is fine.
            }
        }
        for (final ExecutorService executor : executors) {
            executor.shutdownNow();
        }
        for (final ExecutorService executor : executors) {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        }
        return stoppedConsumers;
    }

    private static Map<String, String> getKafkaCommonProperties(final String hostname, final int port,
            final String username, final String password) {
        return Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname + ":" + port,
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        username + "\" password=\"" + password + "\";");
    }

    private static Properties getKafkaAdminClientProperties(final String clientId, final String hostname,
            final int port, final String username, final String password) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000);
        props.putAll(getKafkaCommonProperties(hostname, port, username, password));
        return props;
    }

    private static NewTopic newKafkaTopic(final String topicName) {
        return new NewTopic(topicName, 1, (short) 1);
    }

    private static Set<String> listKafkaTopics(final AdminClient adminClient)
            throws InterruptedException, ExecutionException, TimeoutException {
        final ListTopicsResult listTopicsResult = adminClient.listTopics();
        return listTopicsResult.names().get(5, TimeUnit.SECONDS);
    }

    public static KafkaConsumer<String, byte[]> setupKafkaConsumer(final String hostname, final int port,
            final String username, final String password, final String arbitraryClientId) {
        return new KafkaConsumer<>(getKafkaConsumerProperties(hostname, port, username, password, arbitraryClientId));
    }

    public static KafkaProducer<String, byte[]> setupKafkaProducer(final String hostname, final int port,
            final String username, final String password, final String arbitraryClientId) {
        return new KafkaProducer<>(getKafkaProducerProperties(hostname, port, username, password, arbitraryClientId));
    }

    private static Properties getKafkaConsumerProperties(final String hostname, final int port,
            final String username, final String password, final String arbitraryClientId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaConnectivityIT-consumer-" + arbitraryClientId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.putAll(getKafkaCommonProperties(hostname, port, username, password));
        return props;
    }

    private static Properties getKafkaProducerProperties(final String hostname, final int port,
            final String username, final String password, final String arbitraryClientId) {
        final Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaConnectivityIT-producer-" + arbitraryClientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.putAll(getKafkaCommonProperties(hostname, port, username, password));
        return props;
    }


    @Override
    protected BlockingQueue<ConsumerRecord<String, byte[]>> initResponseConsumer(final String connectionName,
            final String correlationId) {

        return doInitConsumer(connectionName,
                Collections.singleton(replyTargetAddressBuilder.getAddress(connectionName)));
    }

    @Override
    protected BlockingQueue<ConsumerRecord<String, byte[]>> initTargetsConsumer(final String connectionName) {
        return initTargetsConsumer(connectionName, targetAddressBuilder.getAddress(connectionName));
    }

    @Override
    protected BlockingQueue<ConsumerRecord<String, byte[]>> initTargetsConsumer(final String connectionName,
            final String topic) {

        final Set<String> topics = Collections.singleton(topic);
        return doInitConsumer(connectionName, topics);
    }

    private BlockingQueue<ConsumerRecord<String, byte[]>> doInitConsumer(final String connectionId,
            final Collection<String> topics) {

        final BlockingQueue<ConsumerRecord<String, byte[]>> messageQueue = new LinkedBlockingDeque<>();
        logger.info("Subscribing for Kafka topics <{}> in connection <{}>", topics, connectionId);

        final AtomicBoolean partitionsAssigned = new AtomicBoolean(false);

        final KafkaConsumer<String, byte[]> kafkaConsumer = kafkaConsumerFactory.newConsumer();
        kafkaConsumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                logger.info("revoked partitions: {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                logger.info("assigned partitions: {}", partitions);
                partitions.forEach(tp -> logger.info("position for {} is {}", tp, kafkaConsumer.position(tp)));
                partitions.forEach(tp -> logger.info("committed for {} is {}", tp, kafkaConsumer.committed(tp)));
                logger.info("beginningOffsets {}", kafkaConsumer.beginningOffsets(partitions));
                logger.info("endOffsets {}", kafkaConsumer.endOffsets(partitions));
                partitionsAssigned.set(true);
            }
        });

        final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        pollingConsumers.add(Pair.create(executorService, kafkaConsumer));
        executorService.submit(() -> {
            try {
                logger.info("Waiting for Kafka messages on topics '{}' for {} in connection <{}>", topics,
                        kafkaPollTimeout,
                        connectionId);
                boolean polling = true;
                while (polling) {
                    final ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(kafkaPollTimeout);

                    records.forEach(cr -> logger.info("Got record on Kafka topic {} with headers {} and value {}.", cr.topic(),
                            Arrays.stream(cr.headers().toArray())
                                    .map(h -> h.key() + ":" + new String(h.value(), StandardCharsets.UTF_8))
                                    .collect(Collectors.joining(", ")),
                            new String(cr.value(), StandardCharsets.UTF_8)));

                    final AtomicInteger receivedMsgs = new AtomicInteger(0);

                    topics.stream()
                            .flatMap(topic -> {
                                final Iterable<ConsumerRecord<String, byte[]>> topicRecords =
                                        records.records(topic);
                                return StreamSupport.stream(topicRecords.spliterator(), false);
                            })
                            .forEach(cr -> {
                                logger.info("Got records <{}> in connection <{}>", new String(cr.value(), StandardCharsets.UTF_8), connectionId);
                                receivedMsgs.incrementAndGet();
                                messageQueue.add(cr);
                            });

                    if (receivedMsgs.get() == 0) {
                        if (partitionsAssigned.get()) {
                            logger.info("received 0 records - stop polling");
                            polling = false;
                        } else {
                            logger.info("partitions have not been assigned - wait some more...");
                        }
                    }
                }
            } catch (final Exception e) {
                logger.warn("Polling failed: {} {}", e.getClass().getName(), e.getMessage());
            }
        });

        logger.info("Waiting for partitions being assigned for topics: {}", topics);
        Awaitility.await("partitionsAssigned")
                .timeout(waitTimeout.multipliedBy(3).toMillis(), TimeUnit.MILLISECONDS)
                .untilTrue(partitionsAssigned);

        return messageQueue;
    }

    @Override
    protected ConsumerRecord<String, byte[]> consumeResponse(final String correlationId,
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {
        logger.info("Waiting for CommandResponse with correlation-id <{}>", correlationId);
        return pollMessageWithCorrelationId(correlationId, recordQueue);
    }

    @Override
    protected CompletableFuture<ConsumerRecord<String, byte[]>> consumeResponseInFuture(final String correlationId,
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {
        logger.info("Waiting asynchronously for CommandResponse with correlation-id <{}>", correlationId);
        return CompletableFuture.supplyAsync(() -> pollMessageWithCorrelationId(correlationId, recordQueue));
    }

    /**
     * @param correlationId the expected correlation-id
     * @param recordQueue the queue to poll
     * @return the received response
     * @throws java.lang.NullPointerException if the response with the expected correlation id was not received
     * within timeout
     */
    private ConsumerRecord<String, byte[]> pollMessageWithCorrelationId(final String correlationId,
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {

        try {
            ConsumerRecord<String, byte[]> recordFromQueue;
            do {
                recordFromQueue = recordQueue.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } while (!isExpectedResponse(recordFromQueue, correlationId));
            return recordFromQueue;
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private boolean isExpectedResponse(@Nullable final ConsumerRecord<String, byte[]> recordFromQueue,
            final String expectedCorrelationId) {
        requireNonNull(recordFromQueue, String.format("Expected response with correlation-id %s was " +
                "not received within %d seconds.", expectedCorrelationId, waitTimeout.toSeconds()));
        final String actualCorrelationId =
                new String(recordFromQueue.headers().lastHeader(DittoHeaderDefinition.CORRELATION_ID.getKey()).value());
        return expectedCorrelationId.equals(actualCorrelationId);
    }

    @Override
    protected ConsumerRecord<String, byte[]> consumeFromTarget(final String connectionName,
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {

        logger.info("Waiting for Kafka message on topic <{}>", targetAddressBuilder.getAddress(connectionName));
        return doConsumeDelivery(recordQueue);
    }

    @Override
    protected CompletableFuture<ConsumerRecord<String, byte[]>> consumeFromTargetInFuture(final String connectionName,
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {

        logger.info("Waiting asynchronously for Kafka message on topic <{}>", targetAddressBuilder.getAddress(
                connectionName));
        return CompletableFuture.supplyAsync(() -> doConsumeDelivery(recordQueue));
    }

    private ConsumerRecord<String, byte[]> doConsumeDelivery(
            final BlockingQueue<ConsumerRecord<String, byte[]>> recordQueue) {

        try {
            return recordQueue.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected String getCorrelationId(final ConsumerRecord<String, byte[]> record) {
        final Header correlationIdHeader = record.headers().lastHeader("correlation-id");
        if (null != correlationIdHeader) {
            return new String(correlationIdHeader.value(), Charset.defaultCharset());
        } else {
            return "unknown-correlation-id";
        }
    }

    @Override
    protected void sendAsJsonString(final String connectionName, final String correlationId,
            final String stringMessage, final Map<String, String> headersMap) {

        final List<Header> headers = headersMap.entrySet()
                .stream()
                .map(e -> newHeader(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        if (!headersMap.containsKey(DittoHeaderDefinition.CORRELATION_ID.getKey())) {
            headers.add(newHeader(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId));
        }

        final String topic = sourceAddressBuilder.getAddress(connectionName);

        final ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, null, connectionName, stringMessage.getBytes(StandardCharsets.UTF_8),
                        headers);
        try {
            logger.info("Sending record to Kafka topic {}: {}", topic, stringMessage);
            final RecordMetadata recordMetadata =
                    getKafkaProducer().send(record).get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            logger.debug("Record Metadata: {}", recordMetadata.toString());
        } catch (final Exception e) {
            logger.error("Failed to send message to Kafka topic {}: {}", topic, stringMessage);
        }
    }

    @Override
    protected void sendAsBytePayload(final String connectionName, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {
        final List<Header> headers = headersMap.entrySet()
                .stream()
                .map(e -> newHeader(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        if (!headersMap.containsKey(DittoHeaderDefinition.CORRELATION_ID.getKey())) {
            headers.add(newHeader(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId));
        }

        final String topic = sourceAddressBuilder.getAddress(connectionName);

        final ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, null, connectionName, byteMessage, headers);
        try {
            logger.info("Sending record to topic {}: {}", topic, record);
            final RecordMetadata recordMetadata =
                    getKafkaProducer().send(record).get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            logger.debug("Record Metadata: {}", recordMetadata.toString());
        } catch (final Exception e) {
            logger.error("Failed to send message to topic {}: {}", topic, record);
        }
    }

    private static RecordHeader newHeader(final String key, final String value) {
        return new RecordHeader(key, value.getBytes());
    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final ConsumerRecord<String, byte[]> delivery) {
        final String body = new String(delivery.value(), StandardCharsets.UTF_8);
        logger.info("Received Kafka record with correlation-id <{}> and body: {}",
                getCorrelationId(delivery), body);

        return jsonifiableAdaptableFrom(body);
    }

    @Override
    protected String textFrom(final ConsumerRecord<String, byte[]> delivery) {
        return new String(delivery.value(), StandardCharsets.UTF_8);
    }

    @Override
    protected byte[] bytesFrom(final ConsumerRecord<String, byte[]> delivery) {
        return delivery.value();
    }

    @Override
    protected Map<String, String> checkHeaders(final ConsumerRecord<String, byte[]> message,
            final Consumer<JsonObject> verifyBlockedHeaders) {

        final Map<String, Object> adjustedHeaders = new HashMap<>(
                StreamSupport.stream(message.headers().spliterator(), false)
                        .collect(Collectors.toMap(Header::key, h -> new String(h.value(), StandardCharsets.UTF_8)))
        );

        final Map<String, String> headers = new HashMap<>(adjustedHeaders.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));

        verifyBlockedHeaders.accept(mapToJsonObject(headers));

        return headers;
    }

    public synchronized KafkaProducer<String, byte[]> getKafkaProducer() {
        if (kafkaProducer == null) {
            kafkaProducer = kafkaProducerFactory.newProducer();
        }
        return kafkaProducer;
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.KAFKA;
    }

    @FunctionalInterface
    interface KafkaConsumerFactory {

        KafkaConsumer<String, byte[]> newConsumer();
    }

    @FunctionalInterface
    interface KafkaProducerFactory {

        KafkaProducer<String, byte[]> newProducer();
    }

    @FunctionalInterface
    interface TargetAddressBuilder {

        String getAddress(final String connectionId);
    }

    @FunctionalInterface
    interface SourceAddressBuilder {

        String getAddress(final String connectionId);
    }

    @FunctionalInterface
    interface ReplyTargetAddressBuilder {

        String getAddress(final String connectionId);
    }
}
