import unittest
from repodata.repomd import RepoMD
from repodata.primary import PrimaryMD
from repodata.primary_db import PrimaryDatabaseMD
from repodata.updateinfo import UpdateInfoMD
from repodata.repository import Repository
from repodata.test.test_updateinfo import KNOWN_UPDATE_TYPES


class TestRepository(unittest.TestCase):
    def setUp(self):
        """Setup example files."""
        repomd = RepoMD("test_data/repodata/repomd.xml")
        primary_db = PrimaryDatabaseMD("test_data/repodata/primary_db.sqlite")
        primary = PrimaryMD("test_data/repodata/primary.xml")
        updateinfo = UpdateInfoMD("test_data/repodata/updateinfo.xml")

        self.repository = Repository("repo_url", repomd, primary_db, updateinfo)
        self.repository_without_updateinfo = Repository("repo_url", repomd, primary_db, None)
        self.repository_primary_xml = Repository("repo_url", repomd, primary, updateinfo)

    def test_counting(self):
        # Package count should be same in all repositories
        self.assertGreater(self.repository.get_package_count(), 0)
        self.assertGreater(self.repository_without_updateinfo.get_package_count(), 0)
        self.assertEqual(self.repository.get_package_count(), self.repository_primary_xml.get_package_count())
        self.assertEqual(self.repository.get_package_count(), self.repository_without_updateinfo.get_package_count())

        # Only repository with updateinfo has more than 0 updates
        self.assertGreater(self.repository.get_update_count(), 0)
        self.assertEqual(self.repository_without_updateinfo.get_update_count(), 0)

        # Re-count updates of all known types
        update_sum = 0
        for update_type in KNOWN_UPDATE_TYPES:
            cnt = self.repository.get_update_count(type=update_type)
            self.assertGreater(cnt, 0)
            update_sum += cnt
        self.assertEqual(update_sum, self.repository.get_update_count())

        # Repository without updateinfo returns 0 regardless of specified update type
        for update_type in KNOWN_UPDATE_TYPES:
            self.assertEqual(self.repository_without_updateinfo.get_update_count(type=update_type), 0)