from cli.logger import SimpleLogger


class Repository:
    def __init__(self, repo_url, repomd, primary, updateinfo):
        self.logger = SimpleLogger()
        self.repo_url = repo_url
        self.repomd = repomd
        self.primary = primary
        self.updateinfo = updateinfo

    def get_package_count(self):
        return self.primary.get_package_count()

    def get_update_count(self, type=None):
        if self.updateinfo:
            if type:
                return len([1 for u in self.updateinfo.list_updates() if u["type"] == type])
            return len(self.updateinfo.list_updates())
        return 0
