import hashlib
import json
import time


class Message(object):
    """
    Abstraction describing a Kafka message.
    """

    def __init__(self, table, data):
        """
        Create a message.

        :param table: Name of the table.
        :param data: JSON-serializable row data.
        """
        self.timestamp = time.time()
        self.table = table
        self.data = data

    def serialize(self):
        """
        Serialize the message into bytes for shipment to Kafka.

        :return: Serialized message bytes.
        """
        return json.dumps({
            'timestamp': self.timestamp,
            'table': self.table,
            'data': dict(self.data),
            'hash': self._data_hash(),
        })

    def _data_hash(self):
        """
        Create a deterministic hash of the row data. Used for consumer-side idempotency.

        :return: Bytes representing a checksum of a row's data.
        """
        return hashlib.sha256(json.dumps(dict(self.data), sort_keys=True)).hexdigest()
