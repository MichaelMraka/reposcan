import os
import sys
from urllib.parse import urljoin

from download.downloader import FileDownloader, DownloadItem
from repodata.repomd import RepoMD

REPODATA_DIR = "repodata/"


def download_repodata(repo_url):
    repodata_url = urljoin(repo_url, REPODATA_DIR)
    repomd_url = urljoin(repodata_url, "repomd.xml")
    print("Repomd URL: %s" % repomd_url)
    downloader = FileDownloader()

    # Get repomd
    downloader.add(DownloadItem(
        source_url=repomd_url,
        target_path="repomd.xml"
    ))
    downloader.run()
    repomd = RepoMD("repomd.xml")

    primary = repomd.get_primary()
    primary_url = urljoin(repo_url, primary["location"])
    downloader.add(DownloadItem(
        source_url=primary_url,
        target_path=os.path.basename(primary["location"])
    ))
    updateinfo = repomd.get_updateinfo()
    updateinfo_url = urljoin(repo_url, updateinfo["location"])
    downloader.add(DownloadItem(
        source_url=updateinfo_url,
        target_path=os.path.basename(updateinfo["location"])
    ))
    downloader.run()


def test():
    downloader = FileDownloader()
    # downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/repodata/repomd.xml")
    item1 = DownloadItem(
        source_url="http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/repodata/repomd.xml",
        target_path="/dev/null")
    item2 = DownloadItem(
        source_url="http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/26/x86_64/repodata/repomd.xml",
        target_path="/dev/null")
    item3 = DownloadItem(
        source_url="http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/25/x86_64/repodata/repomd.xml",
        target_path="/dev/null")
    for _ in range(100):
        downloader.add(item1)
        downloader.add(item2)
        downloader.add(item3)
    downloader.run()


if __name__ == '__main__':
    repo_url = sys.argv[1]
    print("Repo URL: %s" % repo_url)
    download_repodata(repo_url)
