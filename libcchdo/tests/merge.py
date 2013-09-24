"""Test cases for merge.

"""
from tempfile import TemporaryFile, NamedTemporaryFile
from os import unlink

from libcchdo.fns import _decimal, decimal_to_str
from libcchdo.model.datafile import DataFile, DataFileCollection
from libcchdo.db.model.std import Unit
from libcchdo.merge import (
    BOTTLE_KEY_COLS, determine_bottle_keys, different_columns, map_collections,
    merge_collections, merge_datafiles)
from libcchdo.recipes.orderedset import OrderedSet

from libcchdo.tests import BaseTestCase
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
    print 'Missing log lines', repr(lines)
    stream.seek(0)
    print repr(stream.read())
    return False


class TestMerge(BaseTestCase):
    def test_different_columns(self):
        """Columns between two datafiles differ under a wide variety of cases.

        Case 1: Column values are different
        Case 1 corollary: Flag values are different
        Case 2: Units are different
        Case 3: Column not in original
        Case 4: Column not in derivative

        """
        with TemporaryFile() as origin, TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOYYY
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,NITRAT,NITRAT_FLAG_W,NITRIT,DELC14,DELC14_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,3.00,2,10.0,-999.000,9
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,4.00,2,10.0,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOYYY
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,TDN,TDN_FLAG_W,NITRIT,DELC14,DELC14_FLAG_W,PH_SWS,PH_SWS_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,,NMOL/KG,/MILLE,,,
 316N145_9, TRNS1, 574, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,6.00,3,10.0,-999.000,1,-999.0,9
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,5.00,3,10.0,  10.000,9,-999.0,9
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
                (['DELC14', 'DELC14_FLAG_W', 'NITRIT'],
                 # PH_SWS_FLAG_W has underscores inside the parameter name. All
                 # parts need to be included
                 ['PH_SWS', 'PH_SWS_FLAG_W', 'TDN', 'TDN_FLAG_W'],
                 ['NITRAT', 'NITRAT_FLAG_W'],
                 ['EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR',
                  'BTLNBR_FLAG_W', 'LATITUDE', 'LONGITUDE', 'DEPTH',
                  '_DATETIME']),
                different_columns(dforigin, dfderiv,
                    ('EXPOCODE', 'SECT_ID', 'STNNBR', 'CASTNO', 'SAMPNO',
                     'BTLNBR',)))

            lines = [
                "DELC14 differs at origin row 1:\t(None, Decimal('10.000'))",
                "DELC14_FLAG_W differs at origin row 0:\t(9, 1)",
            ]
            self.assertTrue(ensure_lines(lines, self.logstream))

            # Columns are not different if merged results are not different.
            dfo = DataFile()
            dfd = DataFile()

            dfo.create_columns(['CTDPRS', 'CTDOXY'])
            dfo.check_and_replace_parameters()
            dfd.create_columns(['CTDPRS', 'CTDOXY'])
            dfd.check_and_replace_parameters()

            dfo['CTDPRS'].values = [1, 2, 3]
            dfo['CTDOXY'].values = [10, 20, 30]
            dfd['CTDPRS'].values = [3, 2, 1]
            dfd['CTDOXY'].values = [30, 20, 10]

            self.assertEqual(
                ([], [], [], ['CTDPRS', 'CTDOXY']),
                different_columns(dfo, dfd, ('CTDPRS',)))

    def test_integration_merge_btl(self):
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOYYY
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W,PH_SWS,PH_SWS_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,,,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,-999.000,9,11,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,9,22,9
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9,33,9
 316N145_9, TRNS1, 574, 1, 32, 32,2,1000,5,-999.000,9,44,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOYYY
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W,PH_SWS,PH_SWS_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,,,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9,-999.0,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1,-999.0,9
 316N145_9, TRNS1, 574, 1, 34, 34,2,1000,5,-999.000,9,-999.0,9
 316N145_9, TRNS1, 600, 1,  1,  1,2,1000,5,-999.000,9,-999.0,9
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            dfo = DataFile()
            dfd = DataFile()
            btlex.read(dfo, origin)
            btlex.read(dfd, deriv)
            p_different, p_not_in_orig, p_not_in_deriv, p_common = \
                different_columns(dfo, dfd, BOTTLE_KEY_COLS)
            parameters = p_different + p_not_in_orig
            keys = determine_bottle_keys(dfo, dfd)
            self.assertEqual(
                keys, ('EXPOCODE', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR'))
            parameters = list(OrderedSet(parameters) - OrderedSet(keys))

            # Parameters with underscores in them may be confused when matching
            # flags with them. E.g. PH_SWS_FLAG_W should be matched with PH_SWS
            # not PH.
            dfile = merge_datafiles(dfo, dfd, keys, parameters)

            self.assertEqual(dfile['DELC14'][0], _decimal('10.000'))
            self.assertEqual(dfile['DELC14'].flags_woce[1], 1)

            # Header should be the origin file's header
            self.assertNotIn('header 2', dfile.globals['header'])
            self.assertIn('header 1', dfile.globals['header'])
            # Header should contain the merged parameters
            self.assertIn('Merged parameters: PH_SWS, DELC14, DELC14_FLAG_W',
                          dfile.globals['header'])
            # No double new lines
            self.assertNotIn('\n\n', dfile.globals['header'])
            # new line for header is not included in the writers
            self.assertEqual('\n', dfile.globals['header'][-1])

            # Key columns should not have been converted to floats. This happens
            # for some reason if pandas combine/update have been used.
            self.assertEqual(str(dfile['STNNBR'][0]), '574')
            self.assertEqual(str(dfile['CASTNO'][0]), '1')
            self.assertEqual(str(dfile['SAMPNO'][0]), '36')
            self.assertEqual(str(dfile['BTLNBR'][0]), '36')
            self.assertEqual(str(dfile['PH_SWS'][0]), 'None')

            # Extra keys in derivative file should not be merged in.
            self.assertNotIn(600, dfile['STNNBR'])

            # Make sure warning is printed regarding extra key in deriv file.
            lines = [
                ['Key on', 'derivative file does not exist in origin', '600']
            ]
            self.assertTrue(ensure_lines(lines, self.logstream))

    def test_merge_btl_no_common_keys(self):
        """Warn if there are no common keys."""
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv:
            origin.write("""\
BOTTLE,19700101CCHSIOYYY
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
BOTTLE,19700101CCHSIOYYY
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 36, 36,2,1000,5,  10.000,9
 316N145_9, TRNS1, 574, 1, 35, 35,2,1000,5,-999.000,1
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            dfo = DataFile()
            dfd = DataFile()
            btlex.read(dfo, origin)
            btlex.read(dfd, deriv)
            p_different, p_not_in_orig, p_not_in_derip_not_in_deriv, p_common = \
                different_columns(dfo, dfd, BOTTLE_KEY_COLS)
            parameters = p_different + p_not_in_orig
            keys = determine_bottle_keys(dfo, dfd)
            parameters = list(OrderedSet(parameters) - OrderedSet(keys))
            mdf = merge_datafiles(dfo, dfd, keys, parameters)

            # Make sure warning is printed regarding extra key in deriv file.
            lines = [
                'No keys matched',
                'No keys provided to map on.',
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
        # This doubles to make sure derivate columns do not wholesale overwrite
        # the origin column, they must be merged using the row match algo.
        lines = [
            "Changed units for CTDOXY from '' to 'UMOL/KG'",
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))

    def test_diff_decplaces(self):
        """Derivative is still different when decimal places are different."""
        dfo = DataFile()
        dfo.create_columns(['CTDPRS', 'CTDOXY'])
        dfo['CTDPRS'].append(_decimal('1'))
        dfo['CTDOXY'].append(_decimal('0.140'))

        dfd = DataFile()
        dfd.create_columns(['CTDPRS', 'CTDOXY'])
        dfd['CTDPRS'].append(_decimal('1'))
        dfd['CTDOXY'].append(_decimal('0.14'))

        p_different, p_not_in_orig, p_not_in_deriv, p_common = \
            different_columns(dfo, dfd, ['CTDPRS'])
        self.assertEqual(p_different, ['CTDOXY'])

        dfile = merge_datafiles(dfo, dfd, ['CTDPRS'], ['CTDOXY'])
        self.assertEqual(decimal_to_str(dfile['CTDOXY'][0]), '0.14')
        
    def test_merge_datafiles_does_not_create_extra_columns(self):
        """Merge datafiles but don't create extra columns.

        When merging data files, create columns only if they exist in derivative
        and were requested to be merged in.

        Thanks to sescher for finding this.

        """
        df0 = DataFile()
        df0.create_columns(['CTDPRS', 'CTDOXY'])
        df0['CTDPRS'].append(1, 2)
        df0['CTDPRS'].append(2, 2)
        df0['CTDOXY'].append(40, 2)
        df0['CTDOXY'].append(41, 3)

        df1 = DataFile()
        df1.create_columns(['CTDPRS', 'CTDOXY', 'CTDSAL'])
        df1['CTDPRS'].append(2, 2)
        df1['CTDPRS'].append(3, 2)
        df1['CTDOXY'].append(50, 2)
        df1['CTDOXY'].append(51, 3)
        df1['CTDSAL'].append(20, 2)
        df1['CTDSAL'].append(21, 2)

        mdf = merge_datafiles(df0, df1, ['CTDPRS'], ['CTDOXY'])

        with self.assertRaises(KeyError):
            mdf['CTDSAL']
        
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

        with self.assertRaisesRegexp(ValueError,
                                     'No columns selected to merge are different.'):
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
            args.on = None
            merge_ctdex_and_ctdex(args)

            with open(output.name) as fff:
                dfile = DataFile()
                ctdex.read(dfile, fff)
                self.assertEqual(dfile['CTDSAL'].flags_woce, [2, 2])
                self.assertEqual(map(str, dfile['TRANSM'].values), ['4.3348', '4.3334'])
                self.assertEqual(dfile['TRANSM'].flags_woce, [1, 1])
            unlink(output.name)

    def test_functional_scripts_btlex(self):
        """Test merging Bottle Exchange files."""
        from argparse import Namespace
        from libcchdo.scripts import merge_btlex_and_btlex
        with    TemporaryFile() as origin, \
                TemporaryFile() as deriv, \
                NamedTemporaryFile(delete=False) as output:
            origin.write("""\
BOTTLE,19700101CCHSIOYYY
# header 1
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,NITRAT,DELC14,DELC14_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,3.00,-999.000,9
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,4.00,-999.000,9
END_DATA
""")
            origin.flush()
            origin.seek(0)
            deriv.write("""\
BOTTLE,19700101CCHSIOYYY
# header 2
EXPOCODE,SECT_ID,STNNBR,CASTNO,SAMPNO,BTLNBR,BTLNBR_FLAG_W,LATITUDE,LONGITUDE,DATE,TIME,DEPTH,TDN,DELC14,DELC14_FLAG_W
,,,,,,,,,,,METERS,UMOL/KG,/MILLE,
 316N145_9, TRNS1, 574, 1, 14, 34, 2, 0, 0, 19700101, 0000,1000,4.00,-999.000,2
 316N145_9, TRNS1, 574, 1, 15, 35, 2, 0, 0, 19700101, 0000,1000,5.00,-999.000,1
 316N145_9, TRNS1, 574, 1, 16, 36, 2, 0, 0, 19700101, 0000,1000,6.00,  10.000,9
END_DATA
""")
            deriv.flush()
            deriv.seek(0)

            args = Namespace()
            args.origin = origin
            args.derivative = deriv
            args.parameters_to_merge = None
            args.merge_different = True
            args.output = output
            args.on = None
            merge_btlex_and_btlex(args)

            with open(output.name) as fff:
                dfile = DataFile()
                btlex.read(dfile, fff)
                self.assertEqual(map(str, dfile['TDN'].values), ['6.00', '5.00'])
                self.assertEqual(dfile['TDN'].flags_woce, [])
            unlink(output.name)
        lines = [
            "Merging on keys composed of: ('EXPOCODE', 'STNNBR', 'CASTNO', 'SAMPNO', 'BTLNBR')",
        ]
        self.assertTrue(ensure_lines(lines, self.logstream))
