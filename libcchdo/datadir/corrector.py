import os
import sys
from tempfile import mkdtemp
from shutil import copy2, rmtree
from zipfile import ZipFile
from json import load as jload, dump as jdump
from contextlib import closing

from libcchdo import LOG
from libcchdo.config import get_merger_initials, get_merger_name
from libcchdo.datadir.processing import (
    mkdir_working, DirName, add_readme_history_note)
from libcchdo.datadir.readme import Readme
from libcchdo.datadir.filenames import (
    EXPOCODE_FILENAME, README_FILENAME, CRUISE_META_FILENAME,
    PROCESSING_EMAIL_FILENAME)
from libcchdo.formats.netcdf import Dataset
from libcchdo.datadir.util import ReadmeEmail, full_datadir_path
from libcchdo.db.model.legacy import (
    session as lsesh, Document, Cruise, str_list_add,
    ArcticAssignment, BottleDB, ArgoFile, ArgoSubmission, TrackLine, Event,
    CruiseParameterInfo, QueueFile, Submission, SpatialGroup,
    Internal, UnusedTrack, NewTrack, SupportFile, CruiseGroup)


EXPOCODE_CORRECT_EMAIL_TEMPLATE = """\
Dear CCHDO,

This is an automated message.

The cruise for http://cchdo.ucsd.edu/cruise/{expo} was updated by {merger}.

{summary}
A history note ({note_id}) has been made for the attached processing notes.

"""


class ExpocodeCorrectEmail(ReadmeEmail):
    def generate_body(self, expocode, merger, summary, note_id):
        """Insert the summary used in the readme into the body of the email."""
        return EXPOCODE_CORRECT_EMAIL_TEMPLATE.format(
            expo=expocode, merger=merger, summary=summary, note_id=note_id)


class ExpoCodeAliasCorrector(dict):
    """Correct a cruise directory's expocode/lines.

    The main function to call is correct()

    """
    def __init__(self, expocode_map, all_map):
        self.expocode_old = expocode_map[0]
        self.expocode_new = expocode_map[1]
        self.all_map = all_map

    def is_expocode_present(self, sss):
        return self.expocode_old in sss

    def replace_expocode(self, sss, rjust=False):
        """Replaces all occurrences of old expocode in string with new.

        Arguments:
        rjust - (optional) if set and the old expocode is longer than the new
            expocode, the new one will be right justified using leading spaces.

        """
        expocode_new = self.expocode_new
        old_len = len(self.expocode_old)
        if rjust and len(expocode_new) < old_len:
            expocode_new = expocode_new.rjust(old_len)
        return sss.replace(self.expocode_old, expocode_new)

    def replace_mapped(self, sss, rjust=False):
        """Replace all occurrences of mapped items in the string."""
        for old, new in self.all_map.items():
            sss = sss.replace(old, new)
        return sss

    def replace_all(self, sss, rjust=False):
        """Replaces all occurrences of expocode and mapped items in the string.

        """
        sss = self.replace_expocode(sss, rjust=rjust)
        sss = self.replace_mapped(sss, rjust=rjust)
        return sss

    def fix_cruise_dir_name(self, cruisedir, dryrun=True):
        cruise_dir_base = os.path.dirname(cruisedir)
        cruise_dir_name = os.path.basename(cruisedir)

        if not self.is_expocode_present(cruise_dir_name):
            return cruisedir

        cruise_dir_name_new = self.replace_expocode(cruise_dir_name)
        LOG.info(
            u'Renaming cruise directory to {0}'.format(cruise_dir_name_new))

        path_new = os.path.join(cruise_dir_base, cruise_dir_name_new)
        if os.path.exists(path_new):
            msg = u'Path {0!r} already exists'.format(path_new)
            if dryrun:
                LOG.critical(u'DRYRUN {0}'.format(msg))
            else:
                raise OSError(msg)
        if dryrun:
            LOG.info(u'DRYRUN would rename {0} to {1}'.format(
                cruise_dir_name, cruise_dir_name_new))
        else:
            os.rename(cruise_dir_name, cruise_dir_name_new)
        return path_new

    def fix_cruise_dir_db(self, session, oldpath, newpath, dryrun=True):
        documents = session.query(Document).filter(
            Document.ExpoCode == self.expocode_old).all()
        for document in documents:
            document.ExpoCode = self.expocode_new

        documents = session.query(Document).filter(
            Document.FileName.like(oldpath + '%')).all()
        for document in documents:
            document.FileName = document.FileName.replace(oldpath, newpath)

    def fix_expocode_in_db(self, session):
        """Correct the expocode all over the database."""
        models_to_fix_expocode_for = [
            ArcticAssignment, BottleDB, ArgoFile, ArgoSubmission, TrackLine,
            Event, CruiseParameterInfo, QueueFile, Submission,
            SpatialGroup, Internal, UnusedTrack, NewTrack, SupportFile, 
            ]
        for model in models_to_fix_expocode_for:
            items = session.query(model).\
                filter(model.ExpoCode == self.expocode_old).all()
            for item in items:
                try:
                    LOG.info(u'Change {0} {1} expocode'.format(model.__name__, item.id))
                except AttributeError:
                    LOG.info(u'Change {0} {1} expocode'.format(model.__name__, item.ID))
                item.ExpoCode = self.expocode_new

        # Replace expocode in a list of cruises.
        cgrps = session.query(CruiseGroup).\
            filter(CruiseGroup.cruises.like(
                '%{0}%'.format(self.expocode_old))).all()
        for cgrp in cgrps:
            LOG.info(u'Change {0} {1} expocode'.format('CruiseGroup', cgrp.id))
            cgrp.cruises = self.replace_expocode(cgrp.cruises)

    def fix_expocode(self, cruisedir, dryrun=True):
        """Correct the expocode in the ExpoCode file."""
        try:
            with open(os.path.join(cruisedir, EXPOCODE_FILENAME), 'r+') as fff:
                text = fff.read()
                if not self.is_expocode_present(text):
                    return
                if dryrun:
                    LOG.info(u'DRYRUN would update ExpoCode')
                else:
                    text = self.replace_expocode(text)
                    fff.seek(0)
                    fff.write(text)
                    fff.truncate()
                    LOG.info(u'Updated ExpoCode')
        except IOError, e:
            LOG.error(u'Cruise directory is missing ExpoCode file')
            raise e

    def fix_cruise_json(self, cruisedir, dryrun=True):
        """Correct the expocode in the cruise.json file."""
        try:
            json_path = os.path.join(cruisedir, CRUISE_META_FILENAME)
            with open(json_path) as fff:
                data = jload(fff)
            try:
                data['expocode'] = self.replace_expocode(data['expocode'])
                with open(json_path, 'w') as fff:
                    jdump(data, fff)
                LOG.info(u'Updated cruise json')
            except KeyError:
                pass
        except (OSError, IOError), err:
            LOG.error(u'Could not fix cruise.json file')

    def fix_nc(self, path, dryrun=True, rjust=False):
        # TODO WARNING: Corrupt netcdf files may cause SIGABRT in netCDF library
        rg = Dataset(path, 'r+')
        expocode = getattr(rg, 'EXPOCODE')
        woce_line = getattr(rg, 'WOCE_ID')

        expocode = self.replace_expocode(expocode)
        woce_line = self.replace_all(woce_line)

        setattr(rg, 'EXPOCODE', expocode)
        setattr(rg, 'WOCE_ID', woce_line)
        #LOG.info(u'Updated {0}'.format(path))

    encodings = ['ascii', 'utf8', 'utf16']

    def _determine_encoding(self, text):
        """Determine the text's encoding by trying each decoder in succession.

        """
        for enc in self.encodings:
            try:
                text = text.decode(enc)
                return text, enc
            except UnicodeDecodeError:
                pass
        return None, None

    def fix_text(self, path, dryrun=True, rjust=False):
        with open(path, 'r+') as fff:
            text = fff.read()
            text, encoding = self._determine_encoding(text)
            if encoding is None:
                LOG.warn('Could not decode file {0}.'.format(path))
                return
            text = self.replace_all(text, rjust)
            fff.seek(0)
            fff.write(text.encode(encoding))
            fff.truncate()
            #LOG.info(u'Updated {0}'.format(path))

    def fix_flat(self, path, dryrun=True, rjust=False):
        if path.endswith('.nc'):
            self.fix_nc(path, dryrun, rjust)
        else:
            self.fix_text(path, dryrun, rjust)

    def fix_zip(self, path, dryrun=True, rjust=False):
        """Correct a zip file of flat files."""
        tempdir = mkdtemp()
        info_fpaths = []
        try:
            with ZipFile(path, 'r') as zzz:
                for info in zzz.infolist():
                    fpath = zzz.extract(info, tempdir)
                    self.fix_flat(fpath, dryrun, rjust)
                    info_fpaths.append([info, fpath])

            with ZipFile(path, 'w') as zzz:
                for info, fpath in info_fpaths:
                    info.filename = self.replace_all(info.filename)
                    with open(fpath) as fff:
                        zzz.writestr(info, fff.read())
        finally:
            rmtree(tempdir)

    def correct(self, cruisedir, email_path, dryrun=True, debug=False):
        """Change a cruise directory's ExpoCode and alias.

        This script will correct line and expocodes in
        1. the directory name
        2. the directory data files

        Since the corrector needs to change directory names and file names, it
        does not operate as a UOW.

        The corrector attempts these fixes in the most direct way possible. For
        text files, direct text substitution and for formatted files, editing
        using provided tools. This should minimize any data changes.

        Of note for the possibility of problems is the Summary file where a
        substituted ExpoCode that is longer than the original could cause
        formatting problems.

        """
        with closing(lsesh()) as session:
            self._correct(
                session, cruisedir, email_path, dryrun=dryrun, debug=True)
            if dryrun:
                session.rollback()
            else:
                session.commit()

    def _correct(self, session, cruisedir, email_path, dryrun=True, debug=False):
        """Change a cruise directory's ExpoCode and alias.

        This script will correct line and expocodes in
        1. the directory name
        2. the directory data files

        Since the corrector needs to change directory names and file names, it
        does not operate as a UOW.

        The corrector attempts these fixes in the most direct way possible. For
        text files, direct text substitution and for formatted files, editing
        using provided tools. This should minimize any data changes.

        Of note for the possibility of problems is the Summary file where a
        substituted ExpoCode that is longer than the original could cause
        formatting problems.

        """
        assert os.path.exists(cruisedir), \
           'Cruise directory {0} does not exist'.format(cruisedir)
        LOG.info(
            u'Changing ExpoCode for cruise directory {dir} from {old!r} to '
            '{new!r}'.format(
                dir=cruisedir, old=self.expocode_old, new=self.expocode_new))

        newpath = self.fix_cruise_dir_name(cruisedir, dryrun)
        # Change cruise entries' ExpoCodes and add old ExpoCode to aliases.
        cruises = session.query(Cruise).\
            filter(Cruise.ExpoCode == self.expocode_old).all()
        for cruise in cruises:
            cruise.ExpoCode = self.expocode_new
            cruise.Alias = str_list_add(cruise.Alias, self.expocode_old)
        self.fix_cruise_dir_db(session, cruisedir, newpath, dryrun)
        self.fix_expocode(newpath, dryrun)
        self.fix_cruise_json(newpath, dryrun)

        # Edit all the files and put them in a working directory
        IGNORED_ENTRIES = [
            EXPOCODE_FILENAME, 'Queue', 'original',
        ]
        if dryrun:
            LOG.info(u'DRYRUN would create working directory')
            if debug:
                origdir = mkdtemp(dir='/tmp')
            else:
                origdir = mkdtemp()
        else:
            origdir = os.path.join(newpath, 'original')
        workdir = mkdir_working(
            origdir, get_merger_initials(), 'expocode_correction')

        # warn about possibility of ASCII file alignment changes
        expo_old_len = len(self.expocode_old)
        expo_new_len = len(self.expocode_new)
        expo_len_diff = expo_new_len - expo_old_len
        if expo_old_len > expo_new_len:
            LOG.info(u'New expocode is {0} characters shorter than old '
                     'expocode. The new expocode has been right justified '
                     'with spaces. This should not cause problems.'.format(
                -expo_len_diff))
        elif expo_old_len < expo_new_len:
            LOG.warn(u'New expocode is {0} characters longer than old '
                     'expocode. This may cause issues with ASCII format '
                     "files' alignment. Please check manually.".format(
                expo_len_diff))

        # move to-be-changed files into originals directory, then into
        # to_go_online where work is done
        originalsdir = os.path.join(workdir, DirName.original)
        tgodir = os.path.join(workdir, DirName.tgo)
        renamed_files = {}
        try:
            for fname in os.listdir(newpath):
                if fname in IGNORED_ENTRIES:
                    continue

                fname_new = self.replace_all(fname)
                if fname != fname_new:
                    LOG.info('Renamed {0}\t-> {1}'.format(fname, fname_new))
                    renamed_files[fname] = fname_new
                filepath = os.path.join(tgodir, fname_new)

                copy2(os.path.join(newpath, fname), originalsdir)
                os.unlink(os.path.join(newpath, fname))
                copy2(os.path.join(originalsdir, fname), filepath)

                if fname_new.endswith('.zip'):
                    self.fix_zip(filepath, rjust=True)
                elif fname_new.endswith('.gif') or fname_new.endswith('.jpg'):
                    LOG.warn(
                        u'Please regenerate {0} manually.'.format(fname_new))
                else:
                    if fname_new[-6:-4] == 'do':
                        if fname_new.endswith('.pdf'):
                            LOG.warn(
                                u'Cannot yet fix PDFs. Please get Publications '
                                'to fix {0}.'.format(filepath))
                        else:
                            self.fix_flat(filepath, rjust=False)
                    else:
                        self.fix_flat(filepath, rjust=True)

            # copy tgo contents into online dir
            for fname in os.listdir(tgodir):
                copy2(os.path.join(tgodir, fname), newpath) 

            # Rewrite filenames for documents
            for oldname, newname in renamed_files.items():
                doc = session.query(Document).\
                    filter(Document.ExpoCode == self.expocode_new).\
                    filter(Document.FileName.like('%{0}'.format(oldname))).\
                    first()
                if not doc:
                    continue
                doc.FileName = doc.FileName.replace(oldname, newname)
            # Rewrite document entry for cruise
            doc = session.query(Document).\
                filter(Document.ExpoCode == self.expocode_new).\
                filter(Document.FileType == 'Directory').first()
            if doc:
                doc.Files = '\n'.join(os.listdir(newpath))

            # Rewrite expocodes in database
            self.fix_expocode_in_db(session)

            # Write Readme
            readme_path = os.path.join(workdir, README_FILENAME)
            if self.all_map:
                alias_map_summary = 'Other names changed:\n\n' + '\n'.join([
                    '- "{0}" changed to "{1}"'.format(old, new) for old, new in
                    self.all_map.items()
                    ])
            else:
                alias_map_summary = ''
            alias_map_summary + '\n'
            summary = (u'ExpoCode changed from {0} to {1}. {0} added as an '
                'alias for the cruise. {2}').format(
                self.expocode_old, self.expocode_new, alias_map_summary)
            readme = Readme(self.expocode_new, summary)
            cruisedir_abspath = full_datadir_path(newpath)
            finalize_sections = u'\n'.join(
                [''] + \
                readme.finalize_sections(
                    os.path.join(os.path.dirname(cruisedir_abspath), workdir),
                    cruisedir_abspath,
                    sorted(renamed_files.values())))
            readme_text = unicode(readme) + finalize_sections
            with open(readme_path, 'w') as fff:
                fff.write(readme_text)

            note_id = add_readme_history_note(
                session, readme_text, self.expocode_new, 'ExpoCode',
                'ExpoCode changed')

            # Send expocode change email
            try:
                subject = 'ExpoCode changed from {0} to {1}'.format(
                    self.expocode_old, self.expocode_new)
                ecemail = ExpocodeCorrectEmail(dryrun)
                ecemail.set_subject(subject)
                ecemail.set_body(ecemail.generate_body(
                    self.expocode_new, get_merger_name(), summary, note_id))
                ecemail.attach_readme(readme_text)
                ecemail.send(email_path)
                session.commit()
                LOG.info(u'Please check documents table for {0} to ensure no '
                         'duplicate Filename entries'.format(self.expocode_new))
            except Exception, err:
                LOG.error(u'Could not send email: {0!r}'.format(err))
                LOG.info(u'rolled back history note')
                session.rollback()
        finally:
            if dryrun:
                if debug:
                    LOG.info(u'working directory: {0}'.format(workdir))
                    raw_input('press enter to cleanup')
                rmtree(origdir)
                LOG.info(u'rolled back history note')
                session.rollback()
