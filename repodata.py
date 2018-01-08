import sys
from urllib.parse import urljoin
import requests

REPODATA_DIR = "repodata/"
CHUNK_SIZE = 1048576


class FileDownloader():
    def __init__(self):
        self.session = requests.Session()

    def reset_session(self):
        self.session.close()
        self.session = requests.Session()

    def download(self, url, target_filename):
        with open(target_filename, "wb") as file_handle:
            with self.session.get(url, stream=True) as response:
                total_length = int(response.headers.get("content-length"))
                downloaded = 0
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        file_handle.write(chunk)
                        downloaded += len(chunk)
                        finished_percent = int(round(downloaded * 100.0 / total_length))
                        sys.stdout.write("\r%s %%" % finished_percent)
                print("")

def download_repodata(repo_url):
    repodata_url = urljoin(repo_url, REPODATA_DIR)
    repomd_url = urljoin(repodata_url, "repomd.xml")
    print("Repomd URL: %s" % repomd_url)
    downloader = FileDownloader()
    downloader.download(repomd_url, "repomd.xml")


if __name__ == '__main__':
    repo_url = sys.argv[1]
    print("Repo URL: %s" % repo_url)
    #download_repodata(repo_url)
    downloader = FileDownloader()
    #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/Packages/w/wireshark-cli-2.4.2-1.fc27.i686.rpm", "15m-binary-file")
    #downloader.reset_session()
    #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/Packages/w/wesnoth-data-1.13.10-1.fc27.noarch.rpm", "303m-binary-file")
    #downloader.reset_session()
    #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/Packages/w/webkitgtk4-2.18.4-1.fc27.x86_64.rpm", "13m-binary-file")
    #downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/Packages/w/wireshark-cli-2.4.2-1.fc27.i686.rpm", "15m-binary-file")
    #downloader.reset_session()
    #downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/Packages/w/wesnoth-data-1.13.10-1.fc27.noarch.rpm", "303m-binary-file")
    #downloader.reset_session()
    #downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/Packages/w/webkitgtk4-2.18.4-1.fc27.x86_64.rpm", "13m-binary-file")
    for _ in range(100):
        downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/27/x86_64/repodata/repomd.xml", "repomd.xml")
        downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/26/x86_64/repodata/repomd.xml", "repomd.xml")
        downloader.download("http://download-node-02.eng.bos.redhat.com/fedora/linux/updates/25/x86_64/repodata/repomd.xml", "repomd.xml")
        #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/repodata/repomd.xml", "repomd.xml")
        #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/26/x86_64/repodata/repomd.xml", "repomd.xml")
        #downloader.download("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/25/x86_64/repodata/repomd.xml", "repomd.xml")
