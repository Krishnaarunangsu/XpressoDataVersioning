from data_versioning.pachyderm_client import PachydermClient

class PachydermRepoManager:
    """
    Manages repos on pachyderm cluster
    """
    def __init__(self):
        #self.logger = XprLogger()
        #self.config = XprConfigParser(config_path)["pachyderm"]
        self.pachyderm_client = self.connect_to_pachyderm()

    def connect_to_pachyderm(self):
        """
        connects to pachyderm cluster and returns a PfsClient connection instance

        :return:
            returns a PfsClient Object
        """
        client = PachydermClient(
            "172.16.3.51",
            30650
        )
        return client


if __name__ =="__main__":
    p = PachydermRepoManager()
    print(p)
