import logging
import time

from repliqate.db.kv import KeyValueStoreClient
from repliqate.db.sql import SQLDBClient
from repliqate.metrics.hook import NoopMetricsHook
from repliqate.metrics.hook import StatsdMetricsHook
from repliqate.stream.message import Message
from repliqate.stream.producer import StreamProducerClient


class ReplicationDaemon(object):
    """
    Daemon for periodically executing the main replication routine.
    """

    def __init__(self, config):
        """
        Create a daemon instance from a valid config object.

        :param config: Config instance.
        """
        self.logger = logging.getLogger('repliqate')

        # Parameters
        self.name = config.get('name')
        self.poll_interval = config.get('replication.poll_interval_sec')
        self.sql_table = config.get('replication.sql_source.table')
        self.sql_primary_key = config.get('replication.sql_source.primary_key')
        self.sql_fields = config.get('replication.sql_source.fields')
        self.sql_limit = config.get('replication.sql_source.limit')

        # Clients
        self.db = SQLDBClient(
            db_uri=config.get('replication.sql_source.uri'),
            table=self.sql_table,
        )
        self.kv = KeyValueStoreClient(
            addr=config.get('redis_addr'),
            prefix='repliqate',
        )
        self.stream = StreamProducerClient(
            brokers=config.get('replication.kafka_target.brokers'),
            topic=config.get('replication.kafka_target.topic'),
        )

        statsd_addr = config.get('statsd_addr')
        if statsd_addr:
            self.metrics = StatsdMetricsHook(self.name, statsd_addr)
            self.logger.debug(
                'statsd address provided; enabling metrics reporting: addr={}'.format(statsd_addr),
            )
        else:
            self.metrics = NoopMetricsHook()
            self.logger.debug('statsd address omitted; disabling metrics reporting')

        self.logger.info('initialized replication daemon successfully')

    def start(self):
        """
        Start the replication routine, continuing indefinitely.
        """
        self.logger.info('starting daemon: poll_interval_sec={}'.format(self.poll_interval))

        while True:
            self._loop_task()

            self.logger.debug(
                'sleeping before next replication: duration_sec={}'.format(self.poll_interval),
            )
            time.sleep(self.poll_interval)

    def close(self):
        """
        Close active connections.
        """
        self.logger.debug('stopping daemon, closing stateful connections')

        self.db.close()
        self.stream.close()

    def _loop_task(self):
        """
        Execute a single iteration of the replication routine.
        """
        kv_closure = self.kv.closure(
            namespace='replication',
            key='offset',
            tags={'name': self.name},
        )

        offset = kv_closure.get() or 0

        try:
            self.logger.debug('querying source with primary key offset: offset={}'.format(offset))
            rows = self.db.query(
                fields=[self.db.field(field) for field in self.sql_fields],
                criteria=self.db.field(self.sql_primary_key) > offset,
                limit=self.sql_limit,
            )
        except Exception as e:
            self.logger.error('sql source read failure: exception={}'.format(e))
            return

        if not rows:
            self.logger.debug('no new rows since last fetch; aborting')
            return

        self.logger.debug('serializing messages from fetched rows: num_rows={}'.format(len(rows)))
        messages = [
            Message(self.sql_table, row).serialize()
            for row in rows
        ]

        for message in messages:
            next_offset = rows[-1][self.sql_primary_key]

            try:
                self.logger.debug('publishing message: message={}'.format(message))
                self.stream.publish(message)
            except Exception as e:
                self.logger.error('kafka publication failure: exception={}'.format(e))
                self.logger.error(
                    'aborting current replication; future replication may reproduce this message',
                )
                return

            self.logger.debug('storing next primary key offset: offset={}'.format(next_offset))
            kv_closure.set(next_offset)