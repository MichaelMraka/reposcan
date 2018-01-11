import unittest
from xml.etree.ElementTree import ParseError
from repodata.primary import PrimaryMD


class TestPrimaryMD(unittest.TestCase):
    def setUp(self):
        """Setup example primary file."""
        self.primary = PrimaryMD("test_data/repomd/primary.xml")

    def _test_package(self, pkg):
        self.assertTrue("name" in pkg)
        self.assertTrue("epoch" in pkg)
        self.assertTrue("ver" in pkg)
        self.assertTrue("rel" in pkg)
        self.assertTrue("arch" in pkg)
        self.assertTrue("checksum_type" in pkg)
        self.assertTrue("checksum" in pkg)

    def test_invalid_file(self):
        with self.assertRaises(ParseError):
            PrimaryMD("/dev/null")

    def test_packages(self):
        pkg_count = self.primary.get_package_count()
        packages = self.primary.list_packages()
        # Package count read from field and number of actually parsed packages should be same
        self.assertEqual(pkg_count, len(packages))
        # Test fields in first package in list
        self._test_package(packages[0])
