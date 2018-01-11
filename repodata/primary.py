import xml.etree.ElementTree as eT

NS = {"primary": "http://linux.duke.edu/metadata/common"}


class PrimaryMD:
    def __init__(self, filename):
        self.tree = eT.parse(filename)
        root = self.tree.getroot()
        self.package_count = int(root.get("packages"))
        self.packages = []
        for child in root.findall("primary:package", NS):
            if child.get("type") == "rpm":
                package = {}
                package["name"] = child.find("primary:name", NS).text
                evr = child.find("primary:version", NS)
                package["epoch"] = evr.get("epoch")
                package["ver"] = evr.get("ver")
                package["rel"] = evr.get("rel")
                package["arch"] = child.find("primary:arch", NS).text
                checksum = child.find("primary:checksum", NS)
                package["checksum_type"] = checksum.get("type")
                package["checksum"] = checksum.text
                self.packages.append(package)

    def get_package_count(self):
        return self.package_count

    def list_packages(self):
        return self.packages
