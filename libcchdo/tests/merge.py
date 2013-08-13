"""Test cases for merge.

"""
from unittest import TestCase
from logging import StreamHandler
from tempfile import TemporaryFile, NamedTemporaryFile
from os import unlink

from libcchdo.fns import _decimal
from libcchdo.model.datafile import DataFile, DataFileCollection
from libcchdo.db.model.std import Unit
from libcchdo.merge import (
    Merger, different_columns, map_collections, merge_collections,
    merge_datafiles
    )
from libcchdo.log import LOG

from libcchdo.formats.ctd import exchange as ctdex
from libcchdo.formats.bottle import exchange as btlex


def ensure_lines(lines, stream):
    stream.seek(0)
    for line in stream:
        if not lines:
            break
        for query in lines:
            if type(query) is list:
                found = 0
                for qqq in query:
                    if qqq in line:
                        found += 1
                if found == len(query):
                    lines.remove(query)
            else:
                if query in line:
                    lines.remove(query)
    if not lines:
        return True
    print 'Missing log lines', lines
    stream.seek(0)
    print stream.read()
    return False


class TestMerge(TestCase):
    def _unload_handlers(self):
        self.saved_handlers = []
        for handler in LOG.handlers:
            LOG.removeHandler(handler)
            self.saved_handlers.append(handler)

    def _reload_handlers(self):
        for handler in self.saved_handlers:
            LOG.addHandler(handler)
        self.saved_handlers = []
        
    def setUp(self):
        self._unload_handlers()
        self.logstream = TemporaryFile()
        self.loghandler = StreamHandler(self.logstream)
        LOG.addHandler(self.loghandler)

    def tearDown(self):
        LOG.removeHandler(self.loghandler)
        self._reload_handlers()

    def test_different_columns(self):
        with TemporaryFile() as origin, TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOXXX
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,NITRAT,NITRIT,DELC14,DELC14_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,3.00,10.0,-999.000,9
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,4.00,10.0,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOXXX
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,TDN,NITRIT,DELC14,DELC14_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,NMOL/KG,/MILLE,
 316N145_9, TRNS1, 573, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,6.00,10.0,  10.000,9
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,5.00,10.0,-999.000,1
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            dforigin = DataFile()
            dfderiv = DataFile()
            btlex.read(dforigin, origin)
            btlex.read(dfderiv, deriv)
            self.assertEqual(
                # NITRIT comes after because NMOL/KG is not an expected unit and
                # gets pushed to the end when sorting
                (['STNNBR', 'DELC14_FLAG_W', 'NITRIT'],
                 ['TDN'],
                 ['NITRAT'],
                 ['EXPOCODE', 'SECT_ID', 'CASTNO', 'SAMPNO', 'BTLNBR',
                  'BTLNBR_FLAG_W', 'LATITUDE', 'LONGITUDE', 'DEPTH', 'DELC14',
                  '_DATETIME']),
                different_columns(dforigin, dfderiv))

    def test_integration_merge_btl(self):
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOXXX
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 32, 32,2,1000,5,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOXXX
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9
 316N145_9, TRNS1, 600, 1,  1,  1,2,1000,5,-999.000,9
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            merger = Merger(origin, deriv)
            parameters = merger.different_cols()
            mdata = merger.merge(parameters)
            dfile = mdata.convert_to_datafile(parameters)

            self.assertEqual(dfile['DELC14'][0], _decimal('10.000'))
            self.assertEqual(dfile['DELC14'].flags_woce[1], 1)

            # Header should be the origin file's header
            self.assertNotIn('header 2', dfile.globals['header'])
            self.assertIn('header 1', dfile.globals['header'])
            # Header should contain the merged parameters
            self.assertIn('Merged parameters: DELC14, DELC14_FLAG_W', dfile.globals['header'])

            # Key columns should not have been converted to floats. This happens
            # for some reason if pandas combine/update have been used.
            self.assertEqual(str(dfile['STNNBR'][0]), '574')
            self.assertEqual(str(dfile['CASTNO'][0]), '1')
            self.assertEqual(str(dfile['SAMPNO'][0]), '36')
            self.assertEqual(str(dfile['BTLNBR'][0]), '36')

            # Extra keys in derivative file should not be merged in.
            self.assertNotIn(600, dfile['STNNBR'])

            # Make sure warning is printed regarding extra key in deriv file.
            lines = [
                ['600', 'in derivative file is not in origin']
            ]
            self.assertTrue(ensure_lines(lines, self.logstream))

    def test_merge_btl_no_common_keys(self):
        """Warn if there are no common keys."""
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOXXX
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 16, 36,2,1000,5,-999.000,9
 316N145_9, TRNS1, 574, 1, 15, 35,2,1000,5,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOXXX
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            merger = Merger(origin, deriv)
            parameters = merger.different_cols()
            mdata = merger.merge(parameters)
            self.assertEqual(mdata, None)

            # Make sure warning is printed regarding extra key in deriv file.
            lines = [
                'No keys matched'
            ]
            self.assertTrue(ensure_lines(lines, self.logstream))

    def test_map_collections_keep_origin_files(self):
        """When merging collections, make sure to keep origin's files.

        If files from origin were not mapped to deriv's files, keep them in the
        final product.

        Warn if deriv file not in origin collection.

        """
        odfc = DataFileCollection()
        ddfc = DataFileCollection()

        df0 = DataFile()
        df0.globals['EXPOCODE'] = 'a'
        df0.globals['STNNBR'] = 1
        df0.globals['CASTNO'] = 1
        odfc.append(df0)

        df1 = DataFile()
        df1.globals['EXPOCODE'] = 'b'
        df1.globals['STNNBR'] = 1
        df1.globals['CASTNO'] = 1
        ddfc.append(df1)

        dfile_map = map_collections(odfc, ddfc)

        self.assertEqual(dfile_map, [(df0, df0, ('a', 1, 1))])
        lines = [
            "Origin file key ('a', 1, 1) is not present in derivative collection.",
            "Derivative file key ('b', 1, 1) is not present in origin collection.",
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))

    def test_merge_collections(self):
        """When merging collections, map files, then merge mapped files.

        """
        odfc = DataFileCollection()
        ddfc = DataFileCollection()

        df0 = DataFile()
        df0.globals['EXPOCODE'] = 'a'
        df0.globals['STNNBR'] = 1
        df0.globals['CASTNO'] = 1
        df0.create_columns(['CTDPRS', 'NITRAT', 'NITRIT'])
        df0['CTDPRS'].append(1, 2)
        df0['CTDPRS'].append(2, 2)
        df0['NITRAT'].append(10, 2)
        df0['NITRAT'].append(11, 2)
        df0['NITRIT'].append(10, 2)
        df0['NITRIT'].append(11, 2)
        odfc.append(df0)

        df1 = DataFile()
        df1.globals['EXPOCODE'] = 'a'
        df1.globals['STNNBR'] = 1
        df1.globals['CASTNO'] = 1
        df1.create_columns(['CTDPRS', 'NITRAT', 'NITRIT'])
        df1['CTDPRS'].append(1, 2)
        df1['CTDPRS'].append(3, 2)
        df1['NITRAT'].append(20, 2)
        df1['NITRAT'].append(21, 2)
        df1['NITRIT'].append(10, 2)
        df1['NITRIT'].append(11, 2)
        ddfc.append(df1)

        def merger(origin, deriv):
            return merge_datafiles(
                origin, deriv, ['CTDPRS'], ['NITRAT', 'NITRIT'])
        merged_dfc = merge_collections(odfc, ddfc, merger)

        self.assertEqual(merged_dfc.files[0]['CTDPRS'].values, [1, 2])
        self.assertEqual(merged_dfc.files[0]['NITRAT'].values, [20, 11])
        self.assertEqual(merged_dfc.files[0]['NITRIT'].values, [10, 11])

        lines = [
            # df1 has an different CTDPRS record (3)
            'Key on row 1 of derivative file does not exist in origin: (3,)',
            # NITRIT columns are the same
            "Instructed to merge parameters that are not different: ['NITRIT']"
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))

    def test_merge_datafiles(self):
        """Merge datafiles.

        When merging data files, there are two cases to consider:

        Case 1: Adding new column

            If the derivative file has less records, fill in missing records
            with fill values and missing flags.
            
        Case 2: Updating column data

        It should also be possible to specifically only merge flags. Make sure
        if only merging flags to not merge the data.

        Parameter units should be updated from the derivative.

        """
        df0 = DataFile()
        df0.create_columns(['CTDPRS', 'NITRAT', 'NITRIT', 'CTDOXY'])
        df0['CTDPRS'].append(1, 2)
        df0['CTDPRS'].append(2, 2)
        df0['NITRAT'].append(10, 2)
        df0['NITRAT'].append(11, 2)
        df0['NITRIT'].append(30, 5)
        df0['NITRIT'].append(31, 6)
        df0['CTDOXY'].append(40, 2)
        df0['CTDOXY'].append(41, 3)

        df1 = DataFile()
        df1.create_columns(['CTDPRS', 'NITRAT', 'CTDSAL', 'CTDOXY'])
        df1['CTDPRS'].append(2, 2)
        df1['CTDPRS'].append(3, 2)
        df1['CTDSAL'].append(20, 2)
        df1['CTDSAL'].append(21, 2)
        df1['NITRAT'].append(12, 4)
        df1['NITRAT'].append(13, 4)
        df1['CTDOXY'].append(40, 2)
        df1['CTDOXY'].append(41, 3)

        df1['CTDOXY'].parameter.units = Unit('UMOL/KG')

        self._reload_handlers()

        # Case 1 column add
        mdf = merge_datafiles(
            df0, df1, ['CTDPRS'],
            ['NITRAT', 'NITRAT_FLAG_W', 'CTDSAL', 'CTDSAL_FLAG_W', 'CTDOXY'])
        self.assertEqual(mdf['CTDPRS'].values, [1, 2])
        # Make sure missing values and flags are filled in.
        self.assertEqual(mdf['CTDSAL'].values, [None, 20])
        self.assertEqual(mdf['CTDSAL'].flags_woce, [9, 2])
        # Case 2 data upate
        self.assertEqual(mdf['NITRAT'].values, [10, 12])
        self.assertEqual(mdf['NITRAT'].flags_woce, [9, 4])

        # Columns in origin should be kept
        self.assertEqual(mdf['NITRIT'].values, [30, 31])
        self.assertEqual(mdf['NITRIT'].flags_woce, [5, 6])

        # Units should be overwritten for merged columns
        self.assertEqual(
            mdf['CTDOXY'].parameter.units, df1['CTDOXY'].parameter.units)

        # Make sure warning is printed regarding unit overwrite.
        lines = [
            "Changed units for CTDOXY from '' to 'UMOL/KG'",
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))
        

    def test_merge_datafiles_no_column(self):
        """Error to merge columns in neither datafile."""
        df0 = DataFile()
        df0.create_columns(['CTDPRS', 'NITRAT'])
        df0['CTDPRS'].append(1, 2)
        df0['CTDPRS'].append(2, 2)
        df0['NITRAT'].append(10, 2)
        df0['NITRAT'].append(11, 2)

        df1 = DataFile()
        df1.create_columns(['CTDPRS', 'NITRAT'])
        df1['CTDPRS'].append(1, 2)
        df1['CTDPRS'].append(2, 2)
        df1['NITRAT'].append(20, 3)
        df1['NITRAT'].append(21, 4)

        with self.assertRaisesRegexp(ValueError, 'No columns selected to merge'):
            merge_datafiles(df0, df1, ['CTDPRS'], ['CTDSAL'])
        lines = [
            "Instructed to merge parameters that are not in either datafile: ['CTDSAL']",
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))

    def test_merge_datafiles_flags(self):
        """It should be possible to only merge flag "columns".

        This includes updating and adding flags.
        If adding flags and the original column does not exist, warn and fail.

        """
        df0 = DataFile()
        df0.create_columns(['CTDPRS', 'NITRAT'])
        df0['CTDPRS'].append(1, 2)
        df0['CTDPRS'].append(2, 2)
        df0['NITRAT'].append(10, 2)
        df0['NITRAT'].append(11, 2)

        df1 = DataFile()
        df1.create_columns(['CTDPRS', 'NITRAT'])
        df1['CTDPRS'].append(1, 2)
        df1['CTDPRS'].append(2, 2)
        df1['NITRAT'].append(20, 3)
        df1['NITRAT'].append(21, 4)

        mdf = merge_datafiles(df0, df1, ['CTDPRS'], ['NITRAT_FLAG_W'])
        self.assertEqual(mdf['NITRAT'].values, [10, 11])
        self.assertEqual(mdf['NITRAT'].flags_woce, [3, 4])

    def test_functional_scripts_ctdex(self):
        """Test merging CTD Exchange files."""
        from argparse import Namespace
        from libcchdo.scripts import merge_ctdex_and_ctdex
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv, \
                NamedTemporaryFile(delete=False) as output:
            origin.write("""\
CTD,20120515ODF
# REPORTED CAST DEPTH IS CTD_DEPTH + DISTANCE_ABOVE_BOTTOM AT MAX PRESSURE
NUMBER_HEADERS = 11
EXPOCODE = 33AT20120419
SECT_ID = A20
STNNBR = 1
CASTNO = 1
DATE = 20120421
TIME = 1552
LATITUDE =   6.8682
LONGITUDE =  -53.4793
DEPTH =    66
INSTRUMENT_ID = 796
CTDPRS,CTDPRS_FLAG_W,CTDTMP,CTDTMP_FLAG_W,CTDSAL,CTDSAL_FLAG_W,CTDOXY,CTDOXY_FLAG_W,CTDNOBS,CTDETIME
DBAR,,ITS-90,,PSS-78,,UMOL/KG,,,
      0.0,6,  27.7514,6,  31.2862,6,    229.5,6,        1,    629.9
      2.0,2,  27.7223,2,  31.3925,2,    229.5,2,       11,    640.0
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
CTD,20120515ODF
# REPORTED CAST DEPTH IS CTD_DEPTH + DISTANCE_ABOVE_BOTTOM AT MAX PRESSURE
NUMBER_HEADERS = 11
EXPOCODE = 33AT20120419
SECT_ID = A20
STNNBR = 1
CASTNO = 1
DATE = 20120421
TIME = 1552
LATITUDE =   6.8682
LONGITUDE =  -53.4793
DEPTH =    66
INSTRUMENT_ID = 796
CTDPRS,CTDPRS_FLAG_W,CTDTMP,CTDTMP_FLAG_W,CTDSAL,CTDSAL_FLAG_W,CTDOXY,CTDOXY_FLAG_W,TRANSM,TRANSM_FLAG_W,CTDNOBS,CTDETIME
DBAR,,ITS-90,,PSS-78,,UMOL/KG,,0-5VDC,,,
      0.0,6,  27.7514,6,  31.2862,2,    222.2,6,   4.3348,1,        1,    629.9
      2.0,2,  27.7223,2,  31.3925,2,    229.5,2,   4.3334,1,       11,    640.0
""")
            deriv.flush()
            deriv.seek(0)

            args = Namespace()
            args.origin = origin
            args.derivative = deriv
            args.parameters_to_merge = None
            args.merge_different = True
            args.output = output
            merge_ctdex_and_ctdex(args)

            with open(output.name) as fff:
                dfile = DataFile()
                ctdex.read(dfile, fff)
                self.assertEqual(dfile['CTDSAL'].flags_woce, [2, 2])
                self.assertEqual(map(str, dfile['TRANSM'].values), ['4.3348', '4.3334'])
                self.assertEqual(dfile['TRANSM'].flags_woce, [1, 1])
            unlink(output.name)
