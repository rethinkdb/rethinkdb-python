def decode_utf8(string, encoding='utf-8'):
    if hasattr(string, 'decode'):
        return string.decode(encoding)

    return string


def to_bytes(string, encoding='utf-8', decoding=None):
    """
    Convert string to bytes.
    Compared to Python2 in case of python 3 we must provide encoding.
    """

    string = string.decode(decoding) if decoding and hasattr(string, 'decode') else string

    try:
        value = bytes(string)
    except TypeError:
        value = bytes(string, encoding)


