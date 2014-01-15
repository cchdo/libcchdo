"""00_README.txt for cruise directories.

"""

import os
from datetime import date

from libcchdo.log import LOG, log_above
from libcchdo.datadir.filenames import (
    UOW_CFG_FILENAME, README_TEMPLATE_FILENAME)
from libcchdo.config import get_merger_name_first, get_merger_name_last
from libcchdo.datadir.processing import read_uow_cfg, UOWDirName
from libcchdo.fns import equal_with_epsilon
from libcchdo.formats.woce import get_exwoce_params
from libcchdo.formats.bottle import exchange as btlex
from libcchdo.formats.ctd import exchange as ctdex
from libcchdo.formats.ctd.zip import exchange as ctdzipex
from libcchdo.formats.formats import read_arbitrary, guess_file_type
from libcchdo.formats.exchange import (
    read_type_and_stamp as ex_read_type_and_stamp)
from libcchdo.formats.netcdf import (
    read_type_and_stamp as nc_read_type_and_stamp)


class Table(object):
    def __init__(self, headers, *args):
        self.headers = headers
        self.rows = args

    def __len__(self):
        return len(self.headers)

    def column_width(self, column):
        """Return the widest value for the given column index."""
        max_width = len(self.headers[column])
        for row in self.rows:
            width = len(row[column])
            if width > max_width:
                max_width = width
        return max_width

    @classmethod
    def row(cls, items, column_widths):
        return u' '.join(
            [item.ljust(width) for item, width in zip(items, column_widths)])


class ReST(object):
    """ReStructuredText writing tools."""

    next_footnote_id = 1

    @classmethod
    def toc(cls, levels=2):
        """Table of Contents syntax."""
        return u'.. contents:: :depth: {0}\n'.format(levels)

    @classmethod
    def line(cls, length, character=u'='):
        return character * length

    @classmethod
    def title(cls, text, character=u'=', strips=1):
        """Produce a header.

        strips - the number of lines to draw around the title
        character - the character to draw the line with

        """
        assert strips in [1, 2]
        line = cls.line(len(text), character)
        if strips == 2:
            return u'\n'.join([line, text, line])
        elif strips == 1:
            return u'\n'.join([text, line])
        return u''

    @classmethod
    def table(cls, table):
        assert isinstance(table, Table)

        character = u'='
        col_widths = [table.column_width(col) for col in range(len(table))]

        separator = u' '.join(
            [cls.line(width, character) for width in col_widths])

        body = [
            separator,
            Table.row(table.headers, col_widths),
            separator,
        ]
        for row in table.rows:
            body.append(Table.row(row, col_widths))
        body.append(separator)
        body.append('')

        return u'\n'.join(body)

    @classmethod
    def footnote_note(cls, id):
        return u'[{0}]_'.format(id)

    @classmethod
    def footnote(cls, text, id=None):
        if id is None:
            id = cls.next_footnote_id
            cls.next_footnote_id += 1
        return u'.. [{0}] {1}'.format(id, text)

    @classmethod
    def definition_list(cls, definition_list):
        items = []
        for term, definition in definition_list:
            items.append(u':{0}:\n  {1}'.format(term, definition))
        items.append(u'')
        return u'\n'.join(items)

    @classmethod
    def list(cls, lll):
        items = []
        for item in lll:
            items.append(u'- {0}'.format(item))
        return u'\n'.join(items)

    @classmethod
    def label(cls, text):
        return u'.. _{0}:\n'.format(text)

    @classmethod
    def comment(cls, *lines):
        if len(lines) < 1:
            return u'\n..\n'
        return u'\n'.join(
            [u'.. {0}'.format(lines[0])] +
            [u'   {0}'.format(line) for line in lines[1:]] +
            [u''])


class Readme(object):
    """Generate the boilerplate of a simple 00_README.txt.

    This one is used for fixing Expocodes.

    """
    def __init__(self, expocode, process_text):
        """Initialize the readme document with just an ExpoCode."""
        self.expocode = expocode
        self.process_text = process_text
        self._title = self._gen_title()

    def _gen_title(self):
        """Generate title."""
        return u'{0} processing'.format(self.expocode)

    def title(self):
        """Readme title."""
        return [ReST.title(self._title, '=', strips=2)]

    def header(self):
        """Readme header."""
        return [
            u'',
            u'{0}\n'.format(date.today().strftime('%F')),
            u'{0} {1}\n'.format(
                get_merger_name_first()[0], get_merger_name_last()),
            ReST.toc(),
            ]

    def process(self):
        """Process."""
        parts = [
            ReST.title('Process', '='),
            u'',
            self.process_text
            ]
        return parts

    def directories(self, workdir, cruisedir):
        """Directories."""
        return [
            ReST.title('Directories', '='),
            ReST.definition_list([
                ['working directory', workdir],
                ['cruise directory', cruisedir],
            ]),
            ]

    def updated_files_manifest(self, files):
        """Updated files manifest."""
        rows = []
        for fname in files:
            ftype = guess_file_type(fname)
            fpath = os.path.join(UOWDirName.tgo, fname)
            if ftype in ['btl.ex', 'ctd.ex', 'ctd.zip.ex']:
                with open(fpath, 'r') as fff:
                    dtype, stamp = ex_read_type_and_stamp(fff)
                    rows.append([fname, stamp])
            elif ftype in ['btl.nc', 'ctd.nc', 'btl.zip.nc', 'ctd.zip.nc']:
                with open(fpath, 'r') as fff:
                    dtype, stamp = nc_read_type_and_stamp(fff)
                    rows.append([fname, stamp])
            else:
                LOG.debug(
                    u'Unrecognized file type {0} for filename {1}'.format(
                    ftype, fname))
                rows.append([fname, ''])
                continue
        return [
            ReST.title('Updated Files Manifest', '='),
            ReST.table(Table(
                ['file', 'stamp'], *rows)),
            ]

    def finalize_sections(self, workdir, cruisedir, updated_files):
        """Generate the Directories, and Manifest sections."""
        return self.directories(workdir, cruisedir) + \
            self.updated_files_manifest(updated_files)

    def __unicode__(self):
        parts = []
        parts += self.title()
        parts += self.header()
        parts += self.process()
        return u'\n'.join(parts)


class ProcessingReadme(Readme):
    """Generate the boilerplate of a processing 00_README.txt.

    """
    def __init__(self, uow_dir):
        """Initialize the readme document with the UOW configuration."""
        self.uow_dir = uow_dir
        self.uow_cfg = read_uow_cfg(
            os.path.join(self.uow_dir, UOW_CFG_FILENAME))
        self.submission = self.uow_cfg['q_infos']
        self._title = self._gen_title()

    def _gen_title(self):
        """Generate title."""
        alias = self.uow_cfg['alias']
        expo = self.uow_cfg['expocode']
        data_types_summary = self.uow_cfg['data_types_summary']
        params = self.uow_cfg['params']

        is_empty = lambda x: x

        identifier = u' '.join(filter(is_empty, [alias, expo])) + ' processing'
        return u' - '.join(
            filter(is_empty, [identifier, data_types_summary, params]))
        
    def submissions(self):
        """Submissions table."""
        if not self.submission:
            return []

        rows = []
        for qinfo in self.submission:
            rows.append([
                qinfo['filename'], qinfo['submitted_by'], qinfo['date'],
                qinfo['data_type'], str(qinfo['submission_id']),
                ])
        return [
            ReST.title('Submission', '='),
            u'',
            ReST.table(Table(
                ['filename', 'submitted by', 'date', 'data type', 'id'],
                *rows
            )),
            ]

    @classmethod
    def parameter_list(cls, path, qf_footnote_id, fill_footnote_id,
                       not_in_woce_id):
        """Return a parameter listing for the given file path.

        """
        with open(path) as fff:
            with log_above():
                dfile = read_arbitrary(fff)

        IGNORED_PARAMETERS = [
            'EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'BTLNBR', 'SAMPNO',
            'DEPTH', 'LATITUDE', 'LONGITUDE', '_DATETIME']

        woce_params = get_exwoce_params().keys()
        parameter_list = []
        for column in dfile.sorted_columns():
            if column.parameter.mnemonic_woce() in IGNORED_PARAMETERS:
                continue
            base_param = column.parameter.mnemonic_woce()
            param = base_param
            if column.is_flagged_woce():
                param += ' ' + ReST.footnote_note(qf_footnote_id)
            if (column.values[0] is None and column.is_global()):
                param += ' ' + ReST.footnote_note(fill_footnote_id)
            if base_param not in woce_params:
                param += ' ' + ReST.footnote_note(not_in_woce_id)
            parameter_list.append(param)
        return parameter_list

    def parameters(self):
        """Parameters listing."""
        if not self.submission:
            return []
        file_summaries = []

        qf_footnote = ReST.footnote('parameter has quality flag column')
        qf_footnote_id = ReST.next_footnote_id - 1
        fill_footnote = ReST.footnote(
            'parameter only has fill values/no reported measured data')
        fill_footnote_id = ReST.next_footnote_id - 1
        not_in_woce_footnote = ReST.footnote('not in WOCE bottle file')
        not_in_woce_id = ReST.next_footnote_id - 1

        for qinfo in self.submission:
            fname = qinfo['filename']
            file_summaries.append(ReST.title(fname, '~'))
            file_summaries.append(u'')
            path = os.path.join(
                self.uow_dir, UOWDirName.submission, str(qinfo['q_id']), fname)
            try:
                file_summaries.append(
                    ReST.list(ProcessingReadme.parameter_list(
                        path, qf_footnote_id, fill_footnote_id,
                        not_in_woce_id)))
            except Exception, err:
                LOG.error(
                    u'Unable to read parameters for {0}:\n{1!r}'.format(
                    path, err))
                file_summaries.append(
                    ReST.comment(
                        u'-UOW- Unable to read file. Please fill in manually.'))
        return [ReST.title('Parameters', '-'), u''] + file_summaries + [
            u'',
            qf_footnote,
            fill_footnote,
            not_in_woce_footnote,
            ReST.footnote('merged, see merge_'),
            u'',
            ]

    def process(self):
        """Process."""
        parts = [
            ReST.title('Process', '='),
            u'',
            ReST.comment(u'-UOW- Please fill in the Process section and '
                'delete this comment. Refer to {0} in the UOW as a '
                'guide.'.format(README_TEMPLATE_FILENAME)
            )]
        parts += self.changes()
        parts += self.merge()
        parts += [
            ReST.comment(
                u'-UOW- Conversions, directories and manifest will be '
                'automatically generated on commit.')]
        return parts

    def changes(self):
        """Process changes."""
        return [ReST.title('Changes', '-'), u''] + self.list_files()

    def merge(self):
        """Process merge."""
        return [ReST.label('merge'), ReST.title('Merge', '-'), u''] + \
            self.list_files()

    def list_files(self):
        """Generate a list of file names as section headers."""
        list_files = []
        for qinfo in self.submission:
            fname = qinfo['filename']
            list_files.append(ReST.title(fname, '~'))
        list_files.append(u'')
        return list_files

    def conversions(self):
        """Conversions.

        Make sure that if there are converted files, the merger acknowledges
        that they have been checked in a secondary program for errors.

        """
        try:
            conversions = self.uow_cfg['conversions']
            if not conversions:
                return []
        except KeyError:
            return []
        try:
            checked = self.uow_cfg['conversions_checked']
            if not checked:
                raise ValueError(u'File formats were not checked with '
                                 'secondary program such as JOA.')
        except (KeyError, ValueError), err:
            LOG.error(u'Please check converted file formats and set '
                      '"conversions_checked" to true in {0}'.format(
                      UOW_CFG_FILENAME))
            raise err

        rows = []
        try:
            for fname, fnames, sware in conversions:
                assert type(fnames) == list
                rows.append([fname, u', '.join(fnames), sware])
        except ValueError, err:
            LOG.error(
                u'Conversions list in {0} must be a list of lists.'.format(
                UOW_CFG_FILENAME))
            raise ValueError(u'Conversions list in {0} is malformed.'.format(
                             UOW_CFG_FILENAME))
        except AssertionError:
            LOG.error(
                u'Second element of Conversion list in {0} must be a list of '
                'files that were the source for conversion.'.format(
                UOW_CFG_FILENAME))
            raise ValueError(u'Conversion list in {0} is malformed.'.format(
                UOW_CFG_FILENAME))
        return [
            ReST.title('Conversion', '-'),
            u'',
            ReST.table(Table(
                ['file', 'converted from', 'software'], *rows)),
            u'All converted files opened in JOA with no apparent problems.',
            u'',
            ]

    def finalize_sections(self, workdir, cruisedir, updated_files):
        """Generate the Conversion, Directories, and Manifest sections."""
        return self.conversions() + \
             super(ProcessingReadme, self).finalize_sections(
                workdir, cruisedir, updated_files)

    def __unicode__(self):
        parts = []
        parts += self.title()
        parts += self.header()
        parts += self.submissions()
        parts += self.parameters()
        parts += self.process()
        return u'\n'.join(parts)
