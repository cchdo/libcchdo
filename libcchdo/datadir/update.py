import socket
import os
import re
from datetime import datetime
from contextlib import closing

import transaction
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy import or_

from libcchdo import LOG
from libcchdo.db.model.legacy import Document, Cruise, session
from libcchdo.config import get_datadir_hostname
from libcchdo.datadir.util import full_datadir_path

known_file_types = {
    'hy.txt'   : 'Woce Bottle',
    'su.txt'   : 'Woce Sum',
    'ct.zip'   : 'Woce CTD (Zipped)',
    'sum'      : 'Sum File',
    'ctd$'     : 'CTD File',
    'ct1.zip'  : 'Exchange CTD (Zipped)',
    'ct1.csv'  : 'Exchange CTD',
    'hy1.zip'  : 'Exchange Bottle (Zipped)',
    'hy1.csv$' : 'Exchange Bottle',
    'ctd.zip$'  : 'NetCDF CTD',
    'hyd.zip'  : 'NetCDF Bottle',
    'do.txt'   : 'Documentation',
    'do.pdf'   : 'PDF Documentation',
    'xml'      : 'Directory Description',
    'na.txt'   : 'Coord info',
    'sea'      : 'SEA file',
    'detail.htm'    : 'Data History HTML',
    'person.htm'    : 'Person HTML',
    'type.htm'      : 'Type HTML',
    'datahist.htm'  : 'Data History HTML',
    'trk.jpg'       : 'Small Plot',
    'trk.gif'       : 'Large Plot',
    '.gof'          : 'JGOFS File',
    '.wct'          : 'WCT CTD File',
    'index.htm'     : 'Index HTML File',
    'index_OLD.htm' : 'Old Index HTML File',
    '.gmt'          : 'GMT info File',
    '[^(inv_)]hyd.txt'   : 'Exchange Bottle',
    '.ecp'   : 'French data file',
    '.nav'   : 'Coordinates?',
    '.asc'   : 'Encrypted file',
    '.ps'    : 'Postscript file',
    '.mat'   : 'Matlab file',
    '.lv'    : 'Large Volume file',
    '.lvs'   : 'Large Volume file',
    '$00_README.*/.txt' : 'Citation file',
}
ignored_filenames_startswith = [
        '.'
        ]

def is_expo_in_cruises(sesh, expo):
    """Check to see if gvien expo exists in the cruises table.
    It is ok if expo doesn't exist for this method
    """
    try:
        ddir = sesh.query(Cruise).filter(
                Cruise.ExpoCode == expo
            ).one()
        return True
    except NoResultFound:
        LOG.info( "Given expocode or directory path is not an expocode in the"\
        " cruises table, assuming cruise directory path")
        return False

def guess_path_from_expo(sesh, expo):
    """This method checks the documents table for the "Directory" entry of an
    expocode and uses its 'FileName' (really a path) as the cruise directory
    """
    try:
        ddir = sesh.query(Document).filter(
                    Document.ExpoCode == expo
                ).filter(
                    Document.FileType == "Directory"
                ).one()
        return ddir.FileName
    except NoResultFound:
        raise IOError("No directory entry for ExpoCode {0}, please provide"\
                "full path to cruise directory".format(expo))

def get_ddir_list(ddir):
    """Uses os.walk() to walk the given data dir path and get a list of files
    The file list is then filterd by 'startswith' in the
    ignored_filenames_startswith list. e.g. a "." in the list causes all
    dotfiles to be removed from the file list. The os.walk() method does not
    include . or .. as paths.
    """
    if not os.path.isdir(ddir):
        raise IOError("The path '{0}' does not exist or is not a directory"\
                ", if an ExpoCode was"\
        " provided, the Documents table has a bad entry for that"\
        " expocode".format(ddir))
    files = []
    base_dir = None
    for b, d, f in os.walk(ddir):
        files.extend(f)
        base_dir = b
        break
    
    # remove the ignored filenames from the list of files to process
    to_ignore = []
    for name in files:
        for f in ignored_filenames_startswith:
            if name.startswith(f):
                to_ignore.append(name)
    for name in to_ignore:
        files.remove(name)
    
    return base_dir, files

def identify_files(files):
    """Identifies files based on a regex matching, the regex was taken directly
    from cchdo_update.rb

    In addition, it will warn about duplicated file types
    """
    identified_files = {}
    for key in known_file_types.keys():
        a = re.compile(key)
        for f_name in files:
            if a.search(f_name) is not None:
                identified_files[f_name] = known_file_types[key]
    for f_name in files:
        if f_name not in identified_files:
            identified_files[f_name] = "Unidentified"

    if len(set(identified_files.values())) is not len(files):
        # we have either unidentified files or duplicates, figure out which
        if len(files) is not len(identified_files):
            # There are unidentified files
            f = [x for x in files if x not in identified_files]
            LOG.warn("The following files could not be identified"\
                    " based on file extention: {0}".format(f))
        dups = set([x for x in identified_files.values() if
            identified_files.values().count(x) > 1])
        if len(dups) > 0:
            LOG.warn("There are multiple files of the following types:")
            for dup in dups:
                f = [x for x in identified_files if
                        identified_files[x] == dup]
                LOG.warn( "{0}: {1}".format(dup, f))
            LOG.warn("The update can continue, only one of each file type will"\
            " be displayed on the website")
    return identified_files

def update(expo_or_ddir):
    sesh = session()
    try:
        if get_datadir_hostname() != socket.gethostname():
            raise ValueError("This can only be run on the computer that has the"\
                    " data directory on it: {0}".format(socket.gethostname()))

        # This first block tries to figure out if the input expo_or_ddir is an
        # expocode or a data directory
        ddir = None
        expocode = None
        if is_expo_in_cruises(sesh, expo_or_ddir):
            ddir = guess_path_from_expo(sesh, expo_or_ddir)
        elif ddir is None:
            ddir = full_datadir_path(expo_or_ddir)

        if not ddir.startswith("/data/"):
            # cause why not check to make sure things worked...?
            raise ValueError("The cruise path must absolute and start with"\
                    " /data/")

        base_dir, files = get_ddir_list(ddir)

        # cruise directories are IDed by having an ExpoCode file in them
        if "ExpoCode" not in files:
            raise IOError("No 'ExpoCode' file in the given directory")

        # The expocode as given by the 'ExpoCode' file must actually exist in
        # the cruises table
        with open(os.path.join(base_dir, "ExpoCode")) as expocode_f:
            expocode = expocode_f.readline().strip()
            try: 
                sesh.query(Cruise).filter(Cruise.ExpoCode == expocode).one()
            except NoResultFound:
                raise NoResultFound("The ExpoCode '{0}' as defined in the ExpoCode file"\
                " of the given directory does not exist in the cruises"\
                " table".format(expocode))

        identified_files = identify_files(files)
        
        # The actual update stuff
        documents = sesh.query(Document).filter(
                        Document.ExpoCode == expocode
                    ).filter(
                        or_(Document.FileType != "Directory", Document.FileType == None)
                    ).all()
        old_files = [os.path.basename(d.FileName) for d in documents]
        new_files = []
        unchanged_files = []
        updated_files = []
        deleted_files = []
        for file in old_files:
            if not os.path.exists(os.path.join(base_dir, file)):
                deleted_files.append(file)
        if len(deleted_files) > 0:
            LOG.info("The following have been deleted: {0}".format(deleted_files))
        for file in files:
            full_path = os.path.join(base_dir, file)
            if file not in old_files:
                new_files.append(file)
                continue
            for d in documents:
                if file == os.path.basename(d.FileName):
                    modtime = datetime.fromtimestamp(os.path.getmtime(full_path))
                    if modtime == d.LastModified and full_path == d.FileName:
                        unchanged_files.append(file)
                        continue
                    if modtime < d.LastModified:
                        raise ValueError("The modification date of the file '{0}'"\
                                " is older than what is in the databae".format(file))
                    if modtime > d.LastModified or full_path != d.FileName:
                        updated_files.append((file, d.id))
        # For the updated files, an ID is used because when the file is moved
        # it can't be reliably idenfited by the FileName (really a path)
        for file, id in updated_files:
            full_path = os.path.join(base_dir, file)
            d = sesh.query(Document).filter(Document.id == id).one()
            if file in identified_files:
                d.FileType = identified_files[file]
            moddate = str(datetime.fromtimestamp(os.path.getmtime(full_path)))
            d.LastModified = moddate
            d.Modified = d.Modified + "," + moddate
            d.Size = os.path.getsize(full_path)
            d.FileName = full_path
            sesh.add(d)

        for file in new_files:
            full_path = os.path.join(base_dir, file)
            d = Document()
            d.Size = os.path.getsize(full_path)
            if file in identified_files:
                d.FileType = identified_files[file]
            d.FileName = full_path
            d.ExpoCode = expocode
            moddate = str(datetime.fromtimestamp(os.path.getmtime(full_path)))
            d.LastModified = moddate
            d.Modified = moddate
            d.Stamp = ""
            sesh.add(d)
        
        try:
            d = sesh.query(Document).filter(
                        Document.FileType == "Directory"
                    ).filter(
                        Document.ExpoCode == expocode
                    ).one()
            d.Files = "\n".join(identified_files)
            d.FileName = base_dir
            sesh.add(d)
        except NoResultFound:
            d = Document()
            d.FileType = "Directory"
            d.FileName = base_dir
            d.Files = "\n".join(identified_files)
            d.Stamp = ""
            d.ExpoCode = expocode
            sesh.add(d)

        transaction.commit()
        if len(unchanged_files) > 0:
            LOG.info("The following files are unchanged: {0}".format(unchanged_files))
        if len(new_files) > 0:
            LOG.info("The following files are new: {0}".format(new_files))
        if len(updated_files) > 0:
            LOG.info("The following files are updated: {0}".format([x[0] for x in updated_files]))

        LOG.info('Update done')
    except:
        transaction.rollback()
        raise
    finally:
        sesh.close()
