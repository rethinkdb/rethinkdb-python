import re
import six


def decode_utf8(string, encoding='utf-8'):
    if hasattr(string, 'decode'):
        return string.decode(encoding)

    return string


def chain_to_bytes(*strings):
    return b''.join([six.b(string) if isinstance(string, six.string_types) else string for string in strings])


def get_hostname_for_ssl_match(hostname):
    match = re.match(r'^((?P<subdomain>[^\.]+)\.)?(?P<domain>[^\./]+\.[^/]+)/?.*$', hostname)
    domain = match.group('domain')
    return '*.{domain}'.format(domain=domain) if match.group('subdomain') else domain
