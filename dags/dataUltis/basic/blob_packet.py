class BlobPacket:
    """
    A simple class to hold information required for connecting to Azure Blob Storage.
    """
    def __init__(self, account_url: str, credential: str, container_name: str):
        self.account_url = account_url
        self.credential = credential
        self.container_name = container_name
