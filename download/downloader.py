from threading import Thread
from queue import Queue, Empty
import requests
from cli.logger import EnumerateLogger

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


class FileDownloader:
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

        for t in threads:
            t.join()
