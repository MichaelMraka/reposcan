import xml.etree.ElementTree as eT

NS = {"repo": "http://linux.duke.edu/metadata/repo"}


class RepoMDTypeNotFound(Exception):
    """Raised when certain data type was not parsed from repomd file.  
    """
    pass


class RepoMD:
    def __init__(self, filename):
        """Parse whitelisted fields here.
        """
        self.tree = eT.parse(filename)
        root = self.tree.getroot()
        self.data = {}
        for child in root.findall("repo:data", NS):
            data_type = child.get("type")
            location = child.find("repo:location", NS).get("href")
            size = int(child.find("repo:size", NS).text)
            checksum_type = child.find("repo:checksum", NS).get("type")
            checksum = child.find("repo:checksum", NS).text
            self.data[data_type] = {"location": location, "size": size,
                                    "checksum_type": checksum_type, "checksum": checksum}

    def get_primary(self):
        if "primary" not in self.data:
            raise RepoMDTypeNotFound()
        return self.data["primary"]

    def get_primary_db(self):
        if "primary_db" not in self.data:
            raise RepoMDTypeNotFound()
        return self.data["primary_db"]

    def get_updateinfo(self):
        if "updateinfo" not in self.data:
            raise RepoMDTypeNotFound()
        return self.data["updateinfo"]
