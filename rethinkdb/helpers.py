def decode_utf8(string):
    if hasattr(string, 'decode'):
        return string.decode('utf-8')

    return string
