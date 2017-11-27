import sys
from urllib.parse import urljoin
import requests

REPODATA_DIR = "repodata/"

def download_repodata(repo_url):
    repodata_url = urljoin(repo_url, REPODATA_DIR)
    repomd_url = urljoin(repodata_url, "repomd.xml")
    print("Repomd URL: %s" % repomd_url)
    target_file = "repomd.xml"
    with open(target_file, "wb") as file_handle:
        with requests.get(repomd_url, stream=True) as r:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    file_handle.write(chunk)


if __name__ == '__main__':
    repo_url = sys.argv[1]
    print("Repo URL: %s" % repo_url)
    download_repodata(repo_url)
