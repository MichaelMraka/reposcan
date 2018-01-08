import sys
import time
from urllib.parse import urljoin
from threading import Thread, Lock
from queue import Queue, Empty
import requests

from cli.logger import EnumerateLogger

REPODATA_DIR = "repodata/"
CHUNK_SIZE = 1048576
THREADS = 8


class FileDownloadThread(Thread):
    def __init__(self, queue, logger):
        Thread.__init__(self)
        self.queue = queue
        self.session = requests.Session()
        self.logger = logger

    def _download(self, url, target_filename):
        with open(target_filename, "wb") as file_handle:
            with self.session.get(url, stream=True) as response:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        file_handle.write(chunk)

    def run(self):
        while not self.queue.empty():
            try:
                params = self.queue.get(block=False)
            except Empty:
                break
            self._download(params, "/dev/null")
            self.queue.task_done()
            self.logger.log()

        self.session.close()


class FileDownloader():
    def __init__(self):
        self.queue = Queue()
        self.logger = EnumerateLogger()

    def add(self, url):
        self.queue.put(url)

    def run(self):
        threads = []
        for i in range(THREADS):
            print("Starting thread %d" % i)
            thread = FileDownloadThread(self.queue, self.logger)
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)

        while any(t.isAlive() for t in threads):
            time.sleep(1)
        print("DONE")



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
        #downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/27/x86_64/repodata/repomd.xml")
        #downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/26/x86_64/repodata/repomd.xml")
        #downloader.add("http://download.eng.brq.redhat.com/pub/fedora/linux/updates/25/x86_64/repodata/repomd.xml")
    downloader.run()
