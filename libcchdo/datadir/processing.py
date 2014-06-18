"""Utilities for processing data files and putting them online.

Aims to automate many data directory tasks with an eye on the obsolecense of the
datadir in favor of the website.

"""
import os
import os.path
from datetime import datetime, date
from shutil import copy2
from contextlib import closing
from re import search, sub as re_sub
from urllib2 import urlopen, HTTPError
from json import load as json_load, dump as json_dump, loads
from collections import OrderedDict
from traceback import format_exc

from docutils.utils import SystemMessage
from docutils.core import publish_string

import transaction

from libcchdo import LOG, config
from libcchdo.serve import SimpleHTTPServer
from libcchdo.bb import BB
from libcchdo.fns import uniquify
from libcchdo.formats.formats import file_extensions, guess_file_type
from libcchdo.config import get_merger_name
from libcchdo.formats.google_wire import DefaultJSONSerializer
from libcchdo.datadir.util import (
    mkdir_ensure, make_subdirs, ReadmeEmail, dryrun_log_info, is_cruise_dir,
    str_to_fs_slug, working_dir_name, is_working_dir, copy_chunked, DirName,
    UOWDirName, uow_copy, PERM_STAFF_ONLY_DIR, PERM_STAFF_ONLY_FILE)
from libcchdo.datadir.filenames import (
    EXPOCODE_FILENAME, README_FILENAME, PROCESSING_EMAIL_FILENAME,
    UOW_CFG_FILENAME, README_TEMPLATE_FILENAME, README_FINALIZED_FILENAME)
from libcchdo.datadir.store import LegacyDatastore, PycchdoDatastore


DSTORE = LegacyDatastore()


def copy_replaced(filename, curr_date, separator='_'):
    """Move a replaced file to its special name. DEPRECATED

    Files that were replaced from the main cruise directory used to be renamed
    with an extension and put into the original directory. Now, replaced files
    are put in an originals directory in the work directory that caused the
    change.

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
        ['', 'rplcd', curr_date.strftime('%Y%m%d'), ''])
    extra_extension = extension.split('.')[0]

    new_name = os.path.relpath(os.path.join(dirname, 'original', ''.join(
        [basename, extra_extension, replaced_str, extension])))

    LOG.info('{0} -> {1}'.format(filename, new_name))
    accepted = raw_input('copy? (y/[n]) ')
    if accepted == 'y':
        try:
            copy2(filename, new_name)
        except OSError, e:
            LOG.error(u'Could not move file: {0}'.format(e))
            return 1


def download_url(url, path):
    with open(path, 'w') as ooo:
        with closing(urlopen(url)) as fff:
            copy_chunked(fff, ooo)
    LOG.info(u'downloaded {0}'.format(url))


def populate_dir(dirpath, files, subdirs, dir_perms=0755, file_perms=0644):
    """Create subdirectories and files in directory."""
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


def processing_subdir(name, processing_subdirs=False):
    """Generate subdirectory entry for populating."""
    if processing_subdirs:
        return [name, ['exchange', 'woce', 'netcdf']]
    else:
        return name


def populate_working_dir(
        dirpath, dir_perms=PERM_STAFF_ONLY_DIR, file_perms=PERM_STAFF_ONLY_FILE,
        processing_subdirs=False):
    files = [README_FILENAME]
    subdirs = [
        'submission',
        'to_go_online',
        'originals',
    ]
    subdirs.append(processing_subdir('processing', processing_subdirs))
    populate_dir(dirpath, files, subdirs, dir_perms, file_perms)


def mkdir_working(basepath, person=None, title='working', dtime=None,
                  separator='_', processing_subdirs=False):
    """Create a working directory for data versioning.

    processing_subdirs - (optional) whether to generate subdirectories in the
        processing directory.

    """
    dirpath = os.path.join(
        basepath, working_dir_name(person, title, dtime, separator))
    dir_perms = 0770
    mkdir_ensure(dirpath, dir_perms)
    populate_working_dir(dirpath, dir_perms)
    return dirpath


def get_email_template():
    resp, content = BB.api(
        'GET', '/repositories/ghdc/cchdo/wiki/data_curation_email_templates')
    wiki = loads(content)['data']
    # TODO cut out the template from all the rest
    return wiki


def write_readme_template(template_path):
    """Write the readme template to the given path."""
    template = get_email_template()
    with open(template_path, 'w') as fff:
        fff.write(template.encode('utf8'))


def read_uow_cfg(path):
    with open(path) as fff:
        return json_load(fff)


def write_uow_cfg(path, uow_cfg):
    """Write out a UOW configuration file given the dictionary."""
    try:
        with open(path, 'w') as fff:
            json_dump(uow_cfg, fff, cls=DefaultJSONSerializer, indent=2)
    except IOError, e:
        LOG.error(u'Unable to write {0}'.format(UOW_CFG_FILENAME))
        LOG.info(
            u'You can write your own using this dict {0!r}'.format(uow_cfg))


def is_processing_readme_render_ok(readme_path, confirm_html=True):
    """Ensure that the readme file passes the reST compiler and inspection.

    Arguments:
    confirm_html -- (optional) if set, will attempt to display the rendered page
        to the user and ask for go ahead.

    """
    with open(readme_path) as fff:
        readme = fff.read()

    try:
        output = publish_string(readme, writer_name='html')
    except SystemMessage, err:
        LOG.error(u'{0} failed test: {1!r}'.format(README_FILENAME, err))
        return False

    if confirm_html:
        server = SimpleHTTPServer()
        server.register('/', output)
        server.open_browser()
        accepted = None
        while accepted not in ('y', 'n'):
            server.httpd.handle_request()
            accepted = raw_input('Was the HTML output acceptable? (y/n) ')
            accepted = accepted.lower()
        return accepted == 'y'
    return True


def summarize_submission(q_info):
    return '{0}: {1} {2} {3} {4}'.format(
        q_info['submission_id'], q_info['filename'], q_info['submitted_by'],
        q_info['date'], q_info['data_type'], )


def parse_readme(readme):
    """Parse out salient information from readme file for processing email."""
    title = None
    merger = None
    for line in readme.split('\n'):
        if not title and search('processing', line):
            title = line
        elif not merger and search(r'^\w\s\w+$', line):
            merger = line

    subject = title
    if not merger:
        merger = 'unknown'
    if merger == 'unknown':
        merger = get_merger_name()
    return title, merger, subject


PROCESSING_EMAIL_TEMPLATE = """\
Dear CCHDO,

This is an automated message.

The cruise page for http://cchdo.ucsd.edu/cruise/{expo} was updated by {merger}.

{process_summary}

A history note ({note_id}) has been made for the attached processing notes.
"""


PROCESS_SUMMARY = """\
This update includes:

{sub_plural}
{submission_summary}

{q_plural} {q_ids} marked as merged.\
"""


class ProcessingEmail(ReadmeEmail):
    def generate_body(self, merger, expocode, q_infos, note_id, q_ids):
        sub_ids = uniquify([x['submission_id'] for x in q_infos])
        sub_plural = 'Submissions'
        if len(sub_ids) == 1:
            sub_plural = 'Submission'
        q_plural = 'Queue entries'
        if len(q_ids) == 1:
            q_plural = 'Queue entry'
        submission_summary = '\n'.join(map(summarize_submission, q_infos))
        if q_infos:
            process_summary = PROCESS_SUMMARY.format(sub_plural=sub_plural,
                submission_summary=submission_summary, q_plural=q_plural,
                q_ids=', '.join(map(str, q_ids)))
        else:
            process_summary = ''
        return PROCESSING_EMAIL_TEMPLATE.format(
            expo=expocode, merger=merger, process_summary=process_summary,
            note_id=note_id)


def finalize_readme(readme, fileobj):
    """Generate a final copy of the README with manifests and conversions."""
    tgo_files = os.listdir(os.path.join(readme.uow_dir, UOWDirName.tgo))
    DSTORE.finalize_readme(
        readme, '<remote_work_path>', '<cruise_dir>', tgo_files, fileobj)


def create_processing_email(readme, expocode, q_infos, note_id, q_ids,
                          dryrun=True):
    """Send processing completed notification email."""
    pemail = ProcessingEmail(dryrun=dryrun)
    title, merger, subject = parse_readme(readme)
    pemail.set_subject(subject)
    pemail.set_body(pemail.generate_body(
        merger, expocode, q_infos, note_id, q_ids))
    pemail.attach_readme(readme)
    return pemail


def check_uow_cfg(uow_cfg):
    """Make sure the UOW configuration has required fields."""
    cfg_ok = True
    try:
        uow_cfg['expocode']
    except KeyError:
        LOG.error(u'UOW configuration is missing "expocode". Abort.')
        cfg_ok = False
    try:
        uow_cfg['title']
    except KeyError:
        LOG.error(u'UOW configuration is missing "title". Abort.')
        cfg_ok = False
    try:
        uow_cfg['summary']
    except KeyError:
        LOG.error(u'UOW configuration is missing "summary". Abort.')
        LOG.info(u'Typical entries contain file formats updated e.g.\n'
            'Exchange, NetCDF, WOCE files online\n'
            'Exchange & NetCDF files updated\n'
        )
        cfg_ok = False
    if not cfg_ok:
        raise ValueError(u'UOW configuration is missing required fields.')


def _q_from_uow_cfg(uow_cfg):
    """Retrieve the unique queue file infos and ids from the UOW configuration.

    """
    q_infos = uow_cfg.get('q_infos', [])
    q_ids = uniquify([x['q_id'] for x in q_infos])
    return q_infos, q_ids


class FetchCommitter(object):
    def __init__(self):
        """Create a UOW fetch/committer that operates on the data store."""
        if config.get_option('db', 'dstore', lambda: '') == 'pycchdo':
            dstore = PycchdoDatastore()
        else:
            dstore = LegacyDatastore()
        self.dstore = dstore

    def mkdir_uow(self, basepath, title, summary, ids, separator='_',
                  processing_subdirs=False, dl_originals=True):
        """Create a Unit of Work directory for data work.

        This directory includes the currently online files, submission files,
        and places to put processing, and final files.

        processing_subdirs - (optional) whether to generate subdirectories in
            the processing directory.
        dl_originals - (optional) whether to download the originals directory.

        """
        from libcchdo.datadir.readme import ProcessingReadme

        # Check that all files referenced have the same cruise.
        qfis = self.dstore.as_received_infos(*ids)
        if not qfis:
            LOG.warn(u'None of the ids given refer to Queue files.')
            # fall back to ExpoCode mode
            if len(ids) != 1:
                return
            LOG.info(u'Using id as ExpoCode')
            expocodes = ids
        else:
            expocodes = uniquify([qf['expocode'] for qf in qfis])
        if len(expocodes) > 1:
            LOG.warn(
                u'As-received files do not have the same cruise.\n{0}'.format(
                ', '.join(expocodes)))
            expocode = expocodes[0]
            LOG.info(u'Picked the first cruise as the UOW cruise: {0}'.format(
                expocode))
        elif len(expocodes) == 1:
            expocode = expocodes[0]
        else:
            LOG.error(
                u'None of the as-received files are attached to a cruise. This '
                'must be corrected in the database.')
            return

        dirname = separator.join(
            ['uow', expocode, str_to_fs_slug(title), '-'.join(map(str, ids))])
        dirpath = os.path.join(basepath, dirname)

        dir_perms = 0770
        file_perms = 0660

        mkdir_ensure(dirpath, dir_perms)

        files = [README_FILENAME]
        subdirs = [
            UOWDirName.online,
            UOWDirName.original,
            UOWDirName.submission,
            UOWDirName.tgo,
        ]
        subdirs.append(processing_subdir(
            UOWDirName.processing, processing_subdirs))
        populate_dir(dirpath, files, subdirs, dir_perms, file_perms)

        if expocode != ids[0]:
            qfis = self.dstore.fetch_as_received(
                os.path.join(dirpath, UOWDirName.submission), *ids)
        self.dstore.fetch_online(
            os.path.join(dirpath, UOWDirName.online), expocode)
        if dl_originals:
            self.dstore.fetch_originals(
                os.path.join(dirpath, UOWDirName.original), expocode)

        # Write UOW configuration
        uow_cfg = OrderedDict([
            ['alias', ''],
            ['expocode', expocode],
            ['data_types_summary', ''],
            ['params', ''],
            ['title', title],
            ['q_infos', qfis],
            ['summary', summary],
            ['conversions', []],
            ['conversions_checked', False],
        ])
        write_uow_cfg(os.path.join(dirpath, UOW_CFG_FILENAME), uow_cfg)

        write_readme_template(os.path.join(dirpath, README_TEMPLATE_FILENAME))

        # Generate a preliminary readme file using the UOW configuration.
        readme = ProcessingReadme(dirpath)
        with open(os.path.join(dirpath, README_FILENAME), 'w') as fff:
            fff.write(unicode(readme))

        return dirpath

    def uow_commit(self, uow_dir, person=None, confirm_html=True,
                   send_email=True, dryrun=True):
        """Commit a UOW directory to the cruise data history.

        write 00_README.txt header, submissions, parameter list, conversion,
        directories, and updated manifest

        """
        from libcchdo.datadir.readme import ProcessingReadme
        dryrun_log_info(u'Comitting UOW directory {0}'.format(uow_dir), dryrun)

        dir_perms = 0770

        # pre-flight checks
        # Make sure merger likes readme rendering
        readme_path = os.path.join(uow_dir, README_FILENAME)
        if not is_processing_readme_render_ok(
                readme_path, confirm_html=confirm_html):
            LOG.error(u'README is not valid reST or merger rejected. Stop.')
            return

        # Check UOW configuration
        try:
            readme = ProcessingReadme(uow_dir)
            uow_cfg = readme.uow_cfg
        except IOError:
            LOG.error(
                u'Cannot continue without {0}. (Are you sure {1} is a UOW '
                'directory?)'.format(UOW_CFG_FILENAME, uow_dir))
            return
        except ValueError, err:
            LOG.error(
                u'Unable to read invalid JSON from {0}. Abort.\n{1!r}'.format(
                UOW_CFG_FILENAME, err))
            return
        try:
            check_uow_cfg(uow_cfg)
        except ValueError:
            return
        expocode = uow_cfg['expocode']

        try:
            # Pre-flight checks
            self.dstore.check_cruise_exists(expocode, dir_perms, dryrun)
            self.dstore.check_fetched_online_unchanged(readme)
            finalized_readme_path = os.path.join(
                readme.uow_dir, README_FINALIZED_FILENAME)
            self.dstore.commit(
                readme, person, dir_perms, finalized_readme_path, dryrun)
        except ValueError, err:
            LOG.error(err)
            return

        self.uow_commit_postflight(
            readme, os.path.join(uow_dir, PROCESSING_EMAIL_FILENAME), uow_cfg,
            finalized_readme_path, send_email, dryrun)

        dryrun_log_info(u'UOW commit completed successfully.', dryrun)

    def uow_commit_postflight(self, readme, email_path, uow_cfg,
            finalized_readme_path, send_email=True, dryrun=True):
        """Perform UOW commit postflight actions.

        This includes writing history event, marking queue files merged, general
        bookkeeping and notifying the CCHDO community.

        """
        expocode = uow_cfg['expocode']
        title = uow_cfg['title']
        summary = uow_cfg['summary']
        q_infos, q_ids = _q_from_uow_cfg(uow_cfg)

        note_id = self.dstore.commit_postflight(
            readme, email_path, expocode, title, summary, q_ids,
            finalized_readme_path, dryrun)

        if send_email:
            try:
                pemail = create_processing_email(
                    unicode(readme), expocode, q_infos, note_id, q_ids, dryrun)
                pemail.send(email_path)
            except (KeyboardInterrupt, Exception), err:
                LOG.error(u'Could not send email: {0}'.format(format_exc(3)))
                LOG.info(u'Retry with hydro datadir processing_note')
                transaction.doom()

        if dryrun:
            dryrun_log_info(u'rolled back', dryrun)
            transaction.abort()
        else:
            transaction.commit()
