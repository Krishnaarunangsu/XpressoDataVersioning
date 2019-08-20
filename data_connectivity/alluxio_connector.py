from data_connectivity import alluxio
from data_connectivity.alluxio import *
from io import BytesIO
import pandas as pd
import telnetlib
import xlsxwriter


class AlluxioConnector:
    """

    Sets up Connection with Alluxio
    """
    def __init__(self, host, port):
        """
        Args:
            host: string
            port: int
        """
        self.host = host
        self.port = port

    def connect(self):
        """
        Args:

        Returns:
            client:
        """
        alluxio_client = alluxio.Client(self.host, self.port)
        return alluxio_client


if __name__ == "__main__":
    alx = AlluxioConnector("10.0.23.26", 8080)
    print(alx.connect())
