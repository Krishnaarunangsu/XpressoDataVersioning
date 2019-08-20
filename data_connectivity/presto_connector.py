import prestodb
import pandas as pd
from exception_handling.custom_exception import PrestoConnectionException


class PrestoConnector:
    """ This class is used to interact with the any of the databases as per the configurations provided.
        The supported operation as of now are importing & exporting of data."""

    def __init__(self, presto_ip, presto_port, catalog, schema):
        self.presto_ip = presto_ip
        self.presto_port = presto_port
        self.catalog = catalog
        self.schema = schema

    def get_connector(self):
        """ This methods creates an connection to database with the provided configurations."""
        try:
            connector = prestodb.dbapi.connect(
                    host=self.presto_ip,
                    port=self.presto_port,
                    user="root",
                    catalog=self.catalog,
                    schema=self.schema
                )
            return connector
        except ConnectionError as exc:
            print(exc)
            raise PrestoConnectionException
        except Exception as exc:
            print(exc)
            pass


if __name__ == "__main__":
    pc = PrestoConnector("10.0.23.26", 8080, 'catalog', 'schema')
    print(pc.get_connector())

