import sys
from urllib.parse import urljoin

from download.downloader import FileDownloader

REPODATA_DIR = "repodata/"

def download_repodata(repo_url):
    repodata_url = urljoin(repo_url, REPODATA_DIR)
    repomd_url = urljoin(repodata_url, "repomd.xml")
    print("Repomd URL: %s" % repomd_url)
    downloader = FileDownloader()
    #downloader.download(repomd_url, "repomd.xml")


if __name__ == '__main__':
    repo_url = sys.argv[1]
    print("Repo URL: %s" % repo_url)
    downloader = FileDownloader()
    for _ in range(100):
        downloader.add("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/repodata/repomd.xml")
        downloader.add("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/26/x86_64/repodata/repomd.xml")
        downloader.add("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/25/x86_64/repodata/repomd.xml")
        # downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/repodata/repomd.xml")
        # downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/26/x86_64/repodata/repomd.xml")
        # downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/25/x86_64/repodata/repomd.xml")
    downloader.run()
