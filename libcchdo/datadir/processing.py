import os
from datetime import datetime, date
from shutil import copy2
from contextlib import closing
from re import search
from urllib2 import (
    urlopen,
    HTTPPasswordMgrWithDefaultRealm, HTTPBasicAuthHandler, build_opener,
    Request, HTTPError,
    )
from tempfile import NamedTemporaryFile
from webbrowser import open as webopen

from email import Encoders
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText

from docutils.utils import SystemMessage
from docutils.core import publish_string

from libcchdo import LOG
from libcchdo.fns import file_extensions, guess_file_type
from libcchdo.datadir.util import mkdir_ensure, make_subdirs
from libcchdo.db.model import legacy


EXPOCODE_FILENAME = 'ExpoCode'
README_FILENAME = '00_README.txt'


def mkdir_working(basepath, person, title='working', dt=None, separator='_',
                  processing_subdirs=True):
    """Create a working directory for data work.

    processing_subdirs - (optional) whether to generate subdirectories in the
        processing directory.

        TODO 2013-02-22 these subdirectories are becoming obsolete. Should they
        be removed as an option altogether?

    """
    if not dt:
        dt = date.today()
    dirname = separator.join(
        [dt.strftime('%Y.%m.%d'), title, person])
    dirpath = os.path.join(basepath, dirname)

    dir_perms = 0770
    file_perms = 0660

    mkdir_ensure(dirpath, dir_perms)

    files = [README_FILENAME]
    subdirs = [
        'submission',
        'to_go_online',
    ]
    if processing_subdirs:
        subdirs.append(['processing', ['exchange', 'woce', 'netcdf']])
    else:
        subdirs.append('processing')

    for fname in files:
        fpath = os.path.join(dirpath, fname)
        try:
            os.chmod(fpath, file_perms)
        except OSError:
            pass
        with file(fpath, 'a'):
            os.utime(fpath, None)
            os.chmod(fpath, file_perms)
    make_subdirs(dirpath, subdirs, dir_perms)
    return dirpath


def copy_replaced(filename, date, separator='_'):
    """Move a replaced file to its special name.

    """
    dirname, filename = os.path.split(filename)
    dirname = os.path.join(os.getcwd(), dirname)
    file_type = guess_file_type(filename)
    if file_type is None:
        LOG.error(
            u'File {0} does not have a recognizable file extension.'.format(
            filename))
        return 1

    exts = file_extensions[file_type]
    sorted_exts = sorted(
        zip(exts, map(len, exts)), key=lambda x: x[1], reverse=True)
    exts = [x[0] for x in sorted_exts]

    basename = filename
    extension = None
    for ext in exts:
        if filename.endswith(ext):
            basename = filename[:-len(ext)]
            extension = ext

    replaced_str = separator.join(
        ['', 'rplcd', date.strftime('%Y%m%d'), ''])
    extra_extension = extension.split('.')[0]

    new_name = os.path.relpath(os.path.join(dirname, 'original', ''.join(
        [basename, extra_extension, replaced_str, extension])))

    print filename, '->', new_name
    accepted = raw_input('copy? (y/[n]) ')
    if accepted == 'y':
        try:
            copy2(filename, new_name)
        except OSError, e:
            LOG.error(u'Could not move file: {0}'.format(e))
            return 1


def is_cruise_dir(dir):
    """Determine if the given path is a cruise directory.

    Basically, if an 'ExpoCode' is present.

    """
    return EXPOCODE_FILENAME in os.listdir(dir)


def is_working_dir(dir):
    """Determine if the given path is a working directory.

    Basically, if an '00_README.txt' is present.

    """
    return README_FILENAME in os.listdir(dir)


def write_cruise_dir_expocode(cruise_dir, expocode):
    """Write cruise directory ExpoCode file.

    """
    path = os.path.join(cruise_dir, EXPOCODE_FILENAME)
    with open(path, 'w') as f:
        f.write(expocode + '\n')


def read_cruise_dir_expocode(cruise_dir):
    """Read cruise directory ExpoCode file."""
    path = os.path.join(cruise_dir, EXPOCODE_FILENAME)
    with open(path) as f:
        return f.read().rstrip()


def working_dir_expocode(working_dir):
    """Return the ExpoCode that is tied to the working dir.

    """
    _check_working_dir(working_dir)
    cwd = os.path.abspath(working_dir)
    while not is_cruise_dir(cwd):
        cwd = os.path.dirname(cwd)
    if not cwd or cwd == '/':
        LOG.error(u'Unable to find cruise directory')
        return None
    return read_cruise_dir_expocode(cwd)


def _legacy_fetch_into(ids, submission_dir, hostname='cchdo.ucsd.edu'):
    """Download the as-received files into submission_dir.

    """
    qf_info = []
    with closing(legacy.session()) as sesh:
        qfs = sesh.query(legacy.QueueFile).\
            filter(legacy.QueueFile.id.in_(ids)).all()
        for qf in qfs:
            if qf.is_merged():
                LOG.info(
                    u'QueueFile {0} is marked already merged'.format(qf.id))
            if qf.is_hidden():
                LOG.info(
                    u'QueueFile {0} is marked hidden'.format(qf.id))
            url = 'http://{host}{path}'.format(
                host=hostname, path=qf.unprocessed_input)
            filename = os.path.basename(url)
            submission_subdir = os.path.join(submission_dir, str(qf.id))
            mkdir_ensure(submission_subdir, 0775)
            submission_path = os.path.join(submission_subdir, filename)
            with open(submission_path, 'w') as ooo:
                with closing(urlopen(url)) as fff:
                    chunk = 2 ** 10
                    data = fff.read(chunk)
                    while data:
                        ooo.write(data)
                        data = fff.read(chunk)
            LOG.info(u'downloaded {0}'.format(url))
            qf_info.append({
                'filename': filename,
                'submitted_by': qf.contact,
                'date': qf.date_received,
                'data_type': qf.parameters,
                'id': qf.submission_id,
            })
    return qf_info


def _check_working_dir(working_dir):
    if not is_working_dir(working_dir):
        raise ValueError(
            u'Refusing to fetch into non-working directory {0!r}'.format(
            working_dir))


def fetch_as_received(working_dir, *ids):
    """Copy the referenced as-received files into the working directory.

    """
    _check_working_dir(working_dir)

    submission_dir = os.path.join(working_dir, 'submission')
    qf_info = _legacy_fetch_into(ids, submission_dir)
    print qf_info
    # TODO tamper with 00_README.txt table?


def get_email_template():
    import oauth2 as oauth
    import urlparse

    consumer_key = 'gWRCWSpSQAJNTfXR5d'
    consumer_secret = 'nLLKZe8dkRnJTd4qf4YpDWvsT9JFZtge'

    request_token_url = 'https://bitbucket.org/!api/1.0/oauth/request_token?oauth_callback='
    access_token_url = 'https://bitbucket.org/!api/1.0/oauth/access_token'
    authorize_url = 'https://bitbucket.org/!api/1.0/oauth/authenticate'

    consumer = oauth.Consumer(consumer_key, consumer_secret)
    client = oauth.Client(consumer)

    resp, content = client.request(request_token_url, 'POST')
    if resp['status'] != '200':
        print resp
        raise Exception('Invalid response {0}.'.format(resp['status']))

    request_token = dict(urlparse.parse_qsl(content))

    print "Request Token:"
    print "    - oauth_token        = %s" % request_token['oauth_token']
    print "    - oauth_token_secret = %s" % request_token['oauth_token_secret']
    print request_token
    print 

    # Step 2: Redirect to the provider. Since this is a CLI script we do not 
    # redirect. In a web application you would redirect the user to the URL
    # below.

    authorize_url = "%s?oauth_token=%s" % (authorize_url, request_token['oauth_token'])

    print "Go to the following link in your browser:"
    print authorize_url
    print 

    import webbrowser
    webbrowser.open(authorize_url)

    # After the user has granted access to you, the consumer, the provider will
    # redirect you to whatever URL you have told them to redirect to. You can 
    # usually define this in the oauth_callback argument as well.
    accepted = 'n'
    while accepted.lower() == 'n':
        accepted = raw_input('Have you authorized me? (y/n) ')

    # Step 3: Once the consumer has redirected the user back to the
    # oauth_callback URL you can request the access token the user has approved.
    # You use the request token to sign this request. After this is done you
    # throw away the request token and use the access token returned. You should
    # store this access token somewhere safe, like a database, for future use.

    # FIXME this is broken b/c BB doesn't return an oauth_verifier

    access_token_url += '?oauth_token=' + request_token['oauth_token'] + '&oauth_verifier='
    resp, content = client.request(access_token_url, "POST")
    print resp
    print content
    access_token = dict(urlparse.parse_qsl(content))
    
    print "Access Token:"
    print "    - oauth_token        = %s" % access_token['oauth_token']
    print "    - oauth_token_secret = %s" % access_token['oauth_token_secret']
    print
    print "You may now access protected resources using the access tokens above." 
    print


    api_base = 'https://api.bitbucket.org/1.0'
    api_uri = api_base + ('/repositories/ghdc/cchdo/wiki/'
                          'data_curation_email_templates')


def is_processing_readme_render_ok(working_dir, confirm_html=False):
    """Ensure that the readme file passes the reST compiler and inspection.

    Arguments:
    confirm_html -- (optional) if set, will attempt to display the rendered page
        to the user and ask for go ahead.

    """
    _check_working_dir(working_dir)

    readme_path = os.path.join(working_dir, README_FILENAME)

    with open(readme_path) as fff:
        readme = fff.read()

    try:
        output = publish_string(readme, writer_name='html')
    except SystemMessage, e:
        LOG.error(u'{0} failed test'.format(README_FILENAME))
        return False

    if confirm_html:
        with NamedTemporaryFile() as fff:
            fff.write(output)
            fff.flush()
            fff.seek(0)
            webopen('file://{0}'.format(fff.name))
            accepted = raw_input('Was the HTML output acceptable? (y/n) ')
            return accepted == 'y'
    return True


def processing_email(working_dir):
    """Send processing email.

    """
    recipients = ['cchdo@googlegroups.com']
    # XXX 
    recipients = ['synmantics+test@gmail.com']

    _check_working_dir(working_dir)

    readme_path = os.path.join(working_dir, README_FILENAME)

    with open(readme_path) as fff:
        readme = fff.read()

    title = None
    merger = None
    for line in readme.split('\n'):
        if not title and search('processing', line):
            title = line
        elif not merger and search('^\w\s\w+$', line):
            merger = line

    subject = title
    if not merger:
        merger = 'unknown'
    matches = search('([A-Za-z0-9_\/]+)\s+processing', title)
    expocode = 'unknown'
    if matches:
        expocode = matches.group(1)

    body = """\
Dear CCHDO,

This is an automated message.

The cruise history has been updated by {merger} for http://cchdo.ucsd.edu/cruise/{expo}.

""".format(expo=expocode, merger=merger)

    email = MIMEMultipart()
    email['From'] = 'cchdo@ucsd.edu'
    email['To'] = ', '.join(recipients)
    email['Subject'] = subject

    email.attach(MIMEText(body))

    attachment = MIMEBase('text', 'plain')
    attachment.set_payload(readme)
    Encoders.encode_base64(attachment)
    attachment.add_header(
        'Content-Disposition',
        'attachment; filename="{0}"'.format(README_FILENAME))
    email.attach(attachment)

    print email.as_string()
    #import smtplib
    #s = smtplib.SMTP('localhost')
    #s.sendmail(email['From'], email['To'], email.as_string())
    #s.quit()


def processing_history(working_dir):
    """Add history note for processing directory.

    """
    _check_working_dir(working_dir)

    readme_path = os.path.join(working_dir, README_FILENAME)

    with open(readme_path) as fff:
        readme = fff.read()

    expocode = working_dir_expocode(working_dir)

    with closing(legacy.session()) as sesh:
        cruise = sesh.query(legacy.Cruise).\
            filter(legacy.Cruise.ExpoCode == expocode).first()
        if not cruise:
            LOG.error(
                u'{0} does not refer to a cruise that exists.'.format(expocode))
            return

        event = legacy.Event()
        event.ExpoCode = cruise.ExpoCode
        event.First_Name = ''
        event.LastName = ''
        event.Data_Type = ''
        event.Action = ''
        event.Date_Entered = datetime.now().date()
        event.Summary = ''
        event.Note = readme
        print cruise


def processing_note(working_dir):
    """
    """
    pass
