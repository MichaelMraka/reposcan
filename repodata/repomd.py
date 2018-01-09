import xml.etree.ElementTree as ET

NS = {"repo": "http://linux.duke.edu/metadata/repo"}


class RepoMD:
    def __init__(self, filename):
        """Parse whitelisted fields here.
        """
        self.tree = ET.parse(filename)
        root = self.tree.getroot()
        self.data = {}
        for child in root.findall("repo:data", NS):
            type = child.get("type")
            location = child.find("repo:location", NS).get("href")
            size = int(child.find("repo:size", NS).text)
            checksum_type = child.find("repo:checksum", NS).get("type")
            checksum = child.find("repo:checksum", NS).text
            self.data[type] = {"location": location, "size": size,
                               "checksum_type": checksum_type, "checksum": checksum}

    def get_primary(self):
        return self.data["primary"]

    def get_primary_db(self):
        return self.data["primary_db"]

    def get_updateinfo(self):
        return self.data["updateinfo"]