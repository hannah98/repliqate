import sqlalchemy


class SQLDBClient(object):
    """
    Client for read-only interaction with a SQL database.
    """

    def __init__(self, db_uri, table):
        """
        Create a client instance.

        :param db_uri: SQL URI of the source database.
        :param table: Name of the SQL table in the database.
        """
        engine = sqlalchemy.create_engine(db_uri)

        self._conn = engine.connect()
        self._table = sqlalchemy.Table(
            table,
            sqlalchemy.MetaData(),
            autoload=True,
            autoload_with=engine,
        )

    def query(
        self,
        fields=None,
        criteria=sqlalchemy.sql.expression.true(),
        limit=None,
        offset=None,
        order=None,
    ):
        """
        Perform a read query.

        :param fields: Fields to retrieve; default all.
        :param criteria: Filtering criteria; default no filter.
        :param limit: Limit to the number of documents returned; default none.
        :param offset: Offset in the documents returned; default none.
        :param order: Field to use for ordering of returned results; default none.
        :return: A list of row objects representing the full result of the query.
        """
        if not fields:
            fields = [self._table]

        sql_query = sqlalchemy.select(
            fields
        ).where(
            criteria
        ).limit(
            limit
        ).offset(
            offset
        ).order_by(
            order
        )

        return self._conn.execute(sql_query).fetchall()

    def field(self, name):
        """
        Factory for creating a field object with the specified name.
        :param name:
        :return:
        """
        return self._table.columns[name]

    def close(self):
        """
        Close the underlying connection to the database.
        """
        self._conn.close()
