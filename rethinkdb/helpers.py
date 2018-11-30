def decode_utf8(string, encoding='utf-8'):
    if hasattr(string, 'decode'):
        return string.decode(encoding)

    return string


def to_bytes(string, encoding='utf-8'):
    """
    Convert string to bytes.
    Compared to Python2 in case of python 3 we must provide encoding.
    """

    try:
        return bytes(string)
    except TypeError:
        return bytes(string, encoding)
