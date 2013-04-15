"""THREDDS manipulation.

"""

from urlparse import urljoin

from lxml import etree
from httplib2 import Http


THREDDS_NS = "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"


def _namespace(subns, namespace=THREDDS_NS):
    return './/{{{0}}}{1}'.format(namespace, subns)


def crawl(catalog, recurse=True):
    """Crawl THREDDS catalog recursively for OPENDAP files.

    Adapted from Roberto De Almeida
    https://groups.google.com/d/msg/ioos_tech/eK1eSBkdxJw/J96aBmClTNkJ

    """
    resp, content = Http().request(catalog)
    xml = etree.fromstring(content)

    service = xml.find(_namespace('service[@serviceType="OPENDAP"]'))
    base = service.attrib['base']

    for dataset in xml.iterfind(_namespace('dataset[@urlPath]')):
        yield urljoin(base, dataset.attrib['urlPath'])

    if not recurse:
        return

    for subdir in xml.iterfind(_namespace('catalogRef')):
        subpath = subdir.attrib['{http://www.w3.org/1999/xlink}href']
        suburl = urljoin(catalog, subpath)
        for url in crawl(suburl):
            yield urljoin(suburl, url)
