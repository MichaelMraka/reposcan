import hashlib


def dummy_name(text):
    d = hashlib.sha1(text.encode("utf-8"))
    return d.hexdigest()
