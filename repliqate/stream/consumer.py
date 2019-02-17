import kafka


class StreamConsumerClient(object):
    """
    Client for consuming events from a Kafka stream with automatic offset commits.
    Clients should subclass this class and implement the consume() method.
    """

    def __init__(self, brokers, topic, group='repliqate'):
        """
        Create a consumer client instance.

        :param brokers: List of addresses of Kafka brokers.
        :param topic: Name of the Kafka topic.
        :param group: Name of the consumer group to use.
        """
        self.consumer = kafka.KafkaConsumer(
            topic,
            group_id=group,
            bootstrap_servers=brokers,
            # By default, start consuming at the earliest offset for this topic/group ID
            # combination. This allows clients to consume the full history of the topic, rather than
            # messages published only after the consumer is started.
            auto_offset_reset='earliest',
        )

    def start(self):
        """
        Start consuming messages indefinitely.
        If a deserializer is specified, the consume() method will be invoked with the deserialized
        object.
        """
        for message in self.consumer:
            self.consume(self.deserializer(message.value))

    def deserializer(self, message):
        """
        Optionally specify a mechanism for deserializing the raw (string) message.

        :param message: String message read directly from the Kafka message.
        :return: Deserialized message.
        """
        return message

    def consume(self, message):
        """
        Consume a message.

        :param message: Message read from the Kafka topic, optionally deserialized.
        """
        raise NotImplementedError

    def close(self):
        """
        Close the client connection.
        """
        self.consumer.close()
