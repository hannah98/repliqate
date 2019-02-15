import hashlib
import json
import time


class Message(object):
    """
    Abstraction describing a Kafka message.
    """

    def __init__(self, name, table, data):
        """
        Create a message.

        :param name: Name of the replication job.
        :param table: Name of the table.
        :param data: JSON-serializable row data.
        """
        self.timestamp = time.time()
        self.name = name
        self.table = table
        self.data = data

    def __repr__(self):
        """
        Generate a human-consumable representation of this message.

        :return: String representing this message object.
        """
        return 'Message(timestamp={}, name={}, table={}, data={})'.format(
            self.timestamp,
            self.name,
            self.table,
            dict(self.data),
        )

    def serialize(self):
        """
        Serialize the message into bytes for shipment to Kafka.

        :return: Serialized message bytes.
        """
        return json.dumps({
            # Timestamp at which the message was created by repliqate
            'timestamp': self.timestamp,
            # Name of the replication job
            'name': self.name,
            # Name of the source SQL table
            'table': self.table,
            # Row data serialized as JSON-ish
            'data': dict(self.data),
            # Unique hash of the fetched data
            'hash': self._data_hash(),
        })

    def _data_hash(self):
        """
        Create a deterministic hash of the row data. Used for consumer-side idempotency.

        :return: Bytes representing a checksum of a row's data.
        """
        return hashlib.sha256(json.dumps(dict(self.data), sort_keys=True)).hexdigest()
