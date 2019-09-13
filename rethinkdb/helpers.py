import six


def decode_utf8(string, encoding='utf-8'):
    if hasattr(string, 'decode'):
        return string.decode(encoding)

    return string


def chain_to_bytes(*strings):
    return b''.join([six.b(string) if isinstance(string, six.string_types) else string for string in strings])


def get_hostname_for_ssl_match(hostname):
    parts = hostname.split('.')

    if len(parts) < 3:
        return hostname

    parts[0] = '*'
    return '.'.join(parts)
