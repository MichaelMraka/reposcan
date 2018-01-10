import unittest

from repodata.repomd import RepoMD, RepoMDTypeNotFound


class TestRepoMD(unittest.TestCase):
    def setUp(self):
        """Setup two repomd files. First with all sections. Second with primary section only.
        """
        self.repomd = RepoMD("test_data/repomd/repomd.xml")
        self.repomd_primary_only = RepoMD("test_data/repomd/repomd_primary_only.xml")

    def _test_repomd(self, md):
        self.assertTrue("location" in md)
        self.assertTrue("size" in md)
        self.assertTrue("checksum_type" in md)
        self.assertTrue("checksum" in md)

    def test_get_primary(self):
        primary1 = self.repomd.get_metadata("primary")
        self.assertIsNotNone(primary1)
        self._test_repomd(primary1)

        primary2 = self.repomd_primary_only.get_metadata("primary")
        self.assertIsNotNone(primary2)
        self._test_repomd(primary2)

    def test_get_updateinfo(self):
        updateinfo1 = self.repomd.get_metadata("updateinfo")
        self.assertIsNotNone(updateinfo1)
        self._test_repomd(updateinfo1)

        # Should fail
        with self.assertRaises(RepoMDTypeNotFound):
            self.repomd_primary_only.get_metadata("updateinfo")