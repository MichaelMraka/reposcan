import psycopg2
from psycopg2.extras import execute_values

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
        self.checksum_types = self._lookup_checksum_types()

    def _lookup_checksum_types(self):
        checksums = {}
        cur = self.conn.cursor()
        cur.execute("select id, name from checksum_type")
        for row in cur.fetchall():
            checksums[row[1]] = row[0]
        cur.close()
        return checksums

    def _import_repository(self, repo_url):
        cur = self.conn.cursor()
        cur.execute("select id from repo where name = %s", (dummy_name(repo_url),))
        repo_id = cur.fetchone()
        if not repo_id:
            # FIXME: add product logic
            cur.execute("insert into repo (name, eol) values (%s, false) returning id", (dummy_name(repo_url),))
            repo_id = cur.fetchone()
        cur.close()
        self.conn.commit()
        return repo_id[0]

    def _populate_evrs(self, packages):
        cur = self.conn.cursor()
        evr_map = {}
        unique_evrs = set()
        for pkg in packages:
            unique_evrs.add((pkg["epoch"], pkg["ver"], pkg["rel"]))
        self.logger.log("Unique EVRs in repository: %d" % len(unique_evrs))
        execute_values(cur,
                       """select id, epoch, version, release from evr
                       inner join (values %s) t(epoch, version, release)
                       using (epoch, version, release)""",
                       list(unique_evrs), page_size=len(unique_evrs))
        for row in cur.fetchall():
            evr_map[(row[1], row[2], row[3])] = row[0]
            # Remove to not insert this evr
            unique_evrs.remove((row[1], row[2], row[3]))
        self.logger.log("EVRs already in DB: %d" % len(evr_map))
        self.logger.log("EVRs to import: %d" % len(unique_evrs))
        if unique_evrs:
            # FIXME: insert also evr_t column
            execute_values(cur,
                           """insert into evr (epoch, version, release) values %s
                           returning id, epoch, version, release""",
                           list(unique_evrs), page_size=len(unique_evrs))
            for row in cur.fetchall():
                evr_map[(row[1], row[2], row[3])] = row[0]
        cur.close()
        self.conn.commit()
        return evr_map

    def _populate_packages(self, packages):
        evr_map = self._populate_evrs(packages)
        cur = self.conn.cursor()
        pkg_map = {}
        checksums = set()
        for pkg in packages:
            checksums.add((self.checksum_types[pkg["checksum_type"]], pkg["checksum"]))
        execute_values(cur,
                       """select id, checksum_type_id, checksum from package
                          inner join (values %s) t(checksum_type_id, checksum)
                          using (checksum_type_id, checksum)
                       """, list(checksums), page_size=len(checksums))
        for row in cur.fetchall():
            pkg_map[(row[1], row[2])] = row[0]
            # Remove to not insert this package
            checksums.remove((row[1], row[2]))
        self.logger.log("Packages already in DB: %d" % len(pkg_map))
        self.logger.log("Packages to import: %d" % len(checksums))
        if checksums:
            import_data = []
            for pkg in packages:
                if (self.checksum_types[pkg["checksum_type"]], pkg["checksum"]) in checksums:
                    import_data.append((pkg["name"], evr_map[(pkg["epoch"], pkg["ver"], pkg["rel"])],
                                        self.checksum_types[pkg["checksum_type"]], pkg["checksum"]))
            execute_values(cur,
                           """insert into package (name, evr_id, checksum_type_id, checksum) values %s
                              returning id, checksum_type_id, checksum""",
                              import_data, page_size=len(import_data))
            for row in cur.fetchall():
                pkg_map[(row[1], row[2])] = row[0]
        cur.close()
        self.conn.commit()
        return pkg_map

    def _associate_packages(self, pkg_map, repo_id):
        cur = self.conn.cursor()
        associated_with_repo = set()
        cur.execute("select pkg_id from pkg_repo where repo_id = %s", (repo_id,))
        for row in cur.fetchall():
            associated_with_repo.add(row[0])
        self.logger.log("Packages associated to repository: %d" % len(associated_with_repo))
        to_associate = []
        for pkg_id in pkg_map.values():
            if pkg_id in associated_with_repo:
                associated_with_repo.remove(pkg_id)
            else:
                to_associate.append(pkg_id)
        self.logger.log("New packages to associate with repository: %d" % len(to_associate))
        self.logger.log("Packages to disassociate with repository: %d" % len(associated_with_repo))
        if to_associate:
            execute_values(cur, "insert into pkg_repo (repo_id, pkg_id) values %s",
                           [(repo_id, pkg_id) for pkg_id in to_associate], page_size=len(to_associate))
        # Are there packages to disassociate?
        if associated_with_repo:
            cur.execute("delete from pkg_repo where repo_id = %s and pkg_id in %s",
                        (repo_id, tuple(associated_with_repo),))
        cur.close()
        self.conn.commit()

    def _import_packages(self, repo_id, packages):
        package_map = self._populate_packages(packages)
        self._associate_packages(package_map, repo_id)

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
        cur.close()
        self.conn.commit()
        return severity_id[0]

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
        cur.close()
        self.conn.commit()

    def store(self, repository):
        self.logger.log("Processing repository: %s" % dummy_name(repository.repo_url))
        repo_id = self._import_repository(repository.repo_url)
        self.logger.log("Importing %d packages." % repository.get_package_count())
        self._import_packages(repo_id, repository.list_packages())
        self.logger.log("Importing packages finished.")
        self.logger.log("Importing %d updates." % repository.get_update_count())
        self._import_updates(repo_id, repository.list_updates())
        self.logger.log("Importing updates finished.")
