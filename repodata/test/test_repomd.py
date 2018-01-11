import unittest
from xml.etree.ElementTree import ParseError
from repodata.repomd import RepoMD, RepoMDTypeNotFound


class TestRepoMD(unittest.TestCase):
    def setUp(self):
        """Setup two repomd files. First with all sections. Second with primary section only.
        """
        self.repomd = RepoMD("test_data/repodata/repomd.xml")
        self.repomd_primary_only = RepoMD("test_data/repodata/repomd_primary_only.xml")

    def _test_repomd(self, md):
        self.assertTrue("location" in md)
        self.assertTrue("size" in md)
        self.assertIsInstance(md["size"], int)
        self.assertTrue("checksum_type" in md)
        self.assertTrue("checksum" in md)

    def test_invalid_file(self):
        with self.assertRaises(FileNotFoundError):
            RepoMD("/file/does/not/exist")
        with self.assertRaises(ParseError):
            RepoMD("/dev/null")

    def test_get_primary(self):
        primary1 = self.repomd.get_metadata("primary")
        self._test_repomd(primary1)

        primary2 = self.repomd_primary_only.get_metadata("primary")
        self._test_repomd(primary2)

    def test_get_updateinfo(self):
        updateinfo1 = self.repomd.get_metadata("updateinfo")
        self._test_repomd(updateinfo1)

        # Should fail
        with self.assertRaises(RepoMDTypeNotFound):
            self.repomd_primary_only.get_metadata("updateinfo")
