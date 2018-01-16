import psycopg2

from cli.logger import SimpleLogger
from database.utils import dummy_name

DEFAULT_DB_NAME = "vmaas"
DEFAULT_DB_USER = "vmaasuser"
DEFAULT_DB_PASSWORD = "vmaaspw"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432


class RepositoryStore:
    def __init__(self):
        self.logger = SimpleLogger()
        self.repositories = []
        self.conn = psycopg2.connect(database=DEFAULT_DB_NAME, user=DEFAULT_DB_USER, password=DEFAULT_DB_PASSWORD,
                                     host=DEFAULT_DB_HOST, port=DEFAULT_DB_PORT)

    def _lookup_repository(self, repo_url):
        cur = self.conn.cursor()
        cur.execute("select id from repo where name = %s", (dummy_name(repo_url),))
        repo_id = cur.fetchone()
        if not repo_id:
            # FIXME: add product logic
            cur.execute("insert into repo (name, eol) values (%s, false) returning id", (dummy_name(repo_url),))
            repo_id = cur.fetchone()
            self.conn.commit()
        cur.close()
        return repo_id[0]

    def _lookup_checksum_types(self):
        checksums = {}
        cur = self.conn.cursor()
        cur.execute("select id, name from checksum_type")
        for row in cur.fetchall():
            checksums[row[1]] = row[0]
        cur.close()
        return checksums

    def _lookup_evr(self, epoch, version, release):
        cur = self.conn.cursor()
        cur.execute("select id from evr where epoch = %s and version = %s and release = %s",
                    (epoch, version, release,))
        evr_id = cur.fetchone()
        if not evr_id:
            # FIXME: add evr_t logic
            cur.execute("insert into evr (epoch, version, release) values (%s, %s, %s) returning id",
                        (epoch, version, release,))
            evr_id = cur.fetchone()
            self.conn.commit()
        cur.close()
        return evr_id[0]

    def _lookup_severity(self, severity):
        if severity is None:
            severity = "None"
        cur = self.conn.cursor()
        cur.execute("select id from severity where name = %s", (severity,))
        severity_id = cur.fetchone()
        if not severity_id:
            # FIXME: optimize
            cur.execute("insert into severity (name) values (%s) returning id", (severity,))
            severity_id = cur.fetchone()
            self.conn.commit()
        cur.close()
        return severity_id[0]

    def _import_packages(self, repo_id, packages):
        checksum_types = self._lookup_checksum_types()
        cur = self.conn.cursor()
        # FIXME: optimize
        for pkg in packages:
            evr_id = self._lookup_evr(pkg["epoch"], pkg["ver"], pkg["rel"])
            cur.execute("select id from package where checksum_type_id = %s and checksum = %s",
                        (checksum_types[pkg["checksum_type"]], pkg["checksum"],))
            pkg_id = cur.fetchone()
            if not pkg_id:
                cur.execute("""insert into package (name, evr_id, checksum, checksum_type_id) values (%s, %s, %s, %s)
                               returning id""",
                            (pkg["name"], evr_id, pkg["checksum"], checksum_types[pkg["checksum_type"]]))

                pkg_id = cur.fetchone()
            cur.execute("select 1 from pkg_repo where pkg_id = %s and repo_id = %s", (pkg_id[0], repo_id,))
            in_repo = cur.fetchone()
            if not in_repo:
                cur.execute("insert into pkg_repo (pkg_id, repo_id) values (%s, %s)", (pkg_id[0], repo_id,))
        self.conn.commit()
        cur.close()

    def _import_updates(self, repo_id, updates):
        cur = self.conn.cursor()
        # FIXME: optimize
        for update in updates:
            cur.execute("select id from errata where name = %s", (update["id"],))
            update_id = cur.fetchone()
            if not update_id:
                severity_id = self._lookup_severity(update["severity"])
                cur.execute("insert into errata (name, synopsis, severity_id) values (%s, %s, %s) returning id",
                            (update["id"], update["title"], severity_id,))
                update_id = cur.fetchone()

        self.conn.commit()
        cur.close()

    def store(self, repository):
        repo_id = self._lookup_repository(repository.repo_url)
        self.logger.log("Importing %d packages." % repository.get_package_count())
        self._import_packages(repo_id, repository.list_packages())
        self.logger.log("Importing packages finished.")
        self.logger.log("Importing %d updates." % repository.get_update_count())
        self._import_updates(repo_id, repository.list_updates())
        self.logger.log("Importing updates finished.")
