
# Copyright (C) 2024 Torashin
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# For the full license text, see the COPYING file at the root directory of this project.

import os
import shutil
import time
from datetime import datetime
# from dateutil.parser import parse
import pathlib
import concurrent.futures
import re
from PIL import Image
from pillow_heif import register_heif_opener
import imagehash
import platform
import filetype
import exifread
import datefinder

#import logging
#logging.basicConfig(level=logging.DEBUG)  # Set the logging level according to your preference

if platform.system() == "Windows":
    import exiftool                     # Install PyExifTool
    exiftool_path = os.path.abspath("exiftool.exe")
    os.environ['EXIFTOOL_PATH'] = exiftool_path
    exiftool_supported = True
#elif platform.system() == "Darwin":  # macOS
    #import exiftool
    #exiftool_path = os.path.abspath("exiftool")
    #os.environ['EXIFTOOL_PATH'] = exiftool_path    #This doesn't seem to work
    #Exitool needs to be installed separately (for now)
else:
    try:
        import exiftool
        with exiftool.ExifToolHelper() as et:
            version = et.execute("-ver")
            exiftool_supported = True
    except Exception as e:
        exiftool_supported = False
        print('Please install exiftool for full compatibility')


defaultsourcedir = '~/Desktop/test'
defaultdestdir = '~/Desktop/dest'

# Expand the paths
defaultsourcedir = os.path.expanduser(defaultsourcedir)
defaultdestdir = os.path.expanduser(defaultdestdir)


class FileManager:
    def __init__(self):
        self.file_objects_dict = {}

    def add_file(self, abs_path, base_dir=None, is_in_dest=False, known_year=None, known_month=None):
        if abs_path not in self.file_objects_dict:
            self.file_objects_dict[abs_path] = FileObject(abs_path, base_dir, is_in_dest, known_year, known_month)

    def get_file(self, abs_path, base_dir=None, is_in_dest=False, known_year=None, known_month=None):
        if abs_path not in self.file_objects_dict:
            print(f"Creating FileObject for {abs_path}")
            self.add_file(abs_path, base_dir, is_in_dest, known_year, known_month)
        return self.file_objects_dict.get(abs_path)

class FileObject:
    def __init__(
            self,
            abs_path: str,
            base_dir: str | None = None,
            is_in_dest: bool = False,
            known_year: int | None = None,
            known_month: int | None = None
    ):

        self.base_dir = base_dir
        self.abs_path = abs_path
        self.abs_dir, self.filename = os.path.split(abs_path)
        self.basename, extension = os.path.splitext(self.filename)
        self.extension = extension.lower()
        self._rel_path = None
        self._rel_dir = None
        self._media_type = None
        self._creation_date = None
        self.updated_creation_date = None
        self._modified_date = None
        self._filename_date = None
        self._folder_date = None
        self._meta_date = None
        self._camera_model = None
        self._metadata = None
        self._image_hash = None
        self.decided_date = None
        self.new_basename = None
        self.new_filename = None
        self.new_rel_dir = None
        self.problem_path = '/'
        self.update_meta_date = False
        self.known_year = known_year
        self.known_month = known_month
        self.is_in_dest = is_in_dest
        if is_in_dest:
            self.dest_dir = base_dir
        else:
            self.dest_dir = None

    @property
    def rel_path(self):
        if self._rel_path is None:
            self._rel_path = os.path.relpath(self.abs_path, self.base_dir)
        return self._rel_path

    @property
    def rel_dir(self):
        if self._rel_dir is None:
            self._rel_dir = os.path.relpath(self.abs_dir, self.base_dir)
        return self._rel_dir

    @property
    def media_type(self):
        if self._media_type is None:
            kind = filetype.guess(self.abs_path)
            mime = kind.mime
            self._media_type = mime.split('/')[0]
        return self._media_type

    @property
    def creation_date(self):
        if self._creation_date is None:
            # get creation time in seconds
            ct_sec = os.path.getctime(self.abs_path)
            # convert to date (in wrong format)
            ct_stamp_wrong = time.ctime(ct_sec)
            # Using the timestamp string to create a
            # time object/structure
            ct_obj = time.strptime(ct_stamp_wrong)
            # Transforming the time object to a timestamp
            # of ISO 8601 format
            self._creation_date = time.strftime("%Y-%m-%d %H-%M-%S", ct_obj)
        return self._creation_date

    @property
    def modified_date(self):
        if self._modified_date is None:
            # get modification time in seconds
            mt_sec = os.path.getmtime(self.abs_path)
            # convert to date (in wrong format)
            mt_stamp_wrong = time.ctime(mt_sec)
            # Using the timestamp string to create a
            # time object/structure
            mt_obj = time.strptime(mt_stamp_wrong)
            # Transforming the time object to a timestamp
            # of ISO 8601 format
            self._modified_date = time.strftime("%Y-%m-%d %H-%M-%S", mt_obj)
        return self._modified_date

    @property
    def filename_date(self):
        if self._filename_date is None:
            filename_date = analyse_date(self.basename[:10])
            self._filename_date = filename_date
        return self._filename_date

    @property
    def folder_date(self):
        if self._folder_date is None:
            folderdate = analyse_date(parentdirname(self.abs_path, 1))
            n = 1
            while folderdate == False:
                if n == 4:
                    folderdate = False
                    break
                else:
                    n = n + 1
                folderdate = analyse_date(parentdirname(self.abs_path, n))
            self._folder_date = folderdate
        return self._folder_date

    @property
    def meta_date(self):
        if self._meta_date is None:
            # get date taken from metadata
            if exiftool_supported:
                try:
                    with exiftool.ExifToolHelper() as et:
                        try:
                            tagdata = et.get_tags(self.abs_path, "DateTimeOriginal")
                            tagdata = tagdata[0]['EXIF:DateTimeOriginal']
                        except:
                            try:
                                tagdata = et.get_tags(self.abs_path, "CreateDate")
                                tagdata = tagdata[0]['QuickTime:CreateDate']
                            except:
                                try:
                                    tagdata = et.get_tags(self.abs_path, "MediaCreateDate")
                                    tagdata = tagdata[0]['QuickTime:MediaCreateDate']
                                except:
                                    return False
                except:
                    tagdata = ''
                    print('Can\'t get meta date taken for ' + self.abs_path)
            else:
                if self.media_type == 'image':
                    try:
                        with open(self.abs_path, 'rb') as image_file:
                            tags = exifread.process_file(image_file)
                            tagdata = tags.get('EXIF DateTimeOriginal', None)
                            if tagdata is None:
                                return False
                            else:
                                tagdata = str(tagdata)
                    except Exception as e:
                        tagdata = ''
                        print(f'Error getting meta date taken for {self.abs_path}: \n{e}')
                else:
                    print('Unsupported file format for this OS')
            tagdata = tagdata.replace(":", "-")
            if tagdata == '0000-00-00 00-00-00':
                return False
            self._meta_date = tagdata
        return self._meta_date

    @property
    def camera_model(self):
        if self._camera_model is None:
            # get camera model from metadata
            if exiftool_supported:
                try:
                    with exiftool.ExifToolHelper() as et:
                        tagdata = et.get_tags(self.abs_path, "Model")
                        tagdata = tagdata[0]['EXIF:Model']
                except Exception as e:
                    tagdata = ''
                    print(f'Error getting camera model for {self.abs_path}: \n{e}')
            else:
                if self.media_type == 'image':
                    try:
                        with open(self.abs_path, 'rb') as image_file:
                            tags = exifread.process_file(image_file)
                            tagdata = tags.get('Image Model', None)
                            if tagdata is None:
                                tagdata = ''
                                print('Can\'t get camera model for ' + self.abs_path)
                            else:
                                tagdata = str(tagdata)
                    except Exception as e:
                        tagdata = ''
                        print(f'Error getting camera model for {self.abs_path}: \n{e}')
                else:
                    print('Unsupported file format for this OS')
                    tagdata = ''
            tagdata = tagdata.replace(":", "-")
            self._camera_model = tagdata
        return self._camera_model

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = get_metadata(self.abs_path)[0]
        return self._metadata

    @property
    def new_abs_path(self):
        if self.is_in_dest:
            return self.abs_path
        else:
            return str(self.dest_dir + self.problem_path + self.new_rel_dir + self.new_basename + self.extension)

    @property
    def no_of_ags(self):
        return len(self.metadata)

    @property
    def image_hash(self):
        if not self._image_hash:
            try:
                with Image.open(self.abs_path) as img:  # Open the image file
                    self._image_hash = imagehash.average_hash(img)  # Pass the Image object
            except Exception as e:
                print(f"Error processing image hash for {self.abs_path}: {e}")
                self._image_hash = None  # Handle errors gracefully
        return self._image_hash


def get_metadata(files):
    metadata = []
    if exiftool_supported:
        try:
            with exiftool.ExifToolHelper() as et:
                metadata = et.get_metadata(files)
                return metadata
        except Exception as e:
            print(f'Error reading metadata: \n{e}')
    else:
        print('OS is not Windows - using fallback metadata read function, which will be less extensive')
        if isinstance(files, str):
            files = [files]  # Ensure files is a list
        for file in files:
            if get_media_type(file) == 'image':
                try:
                    with open(file, 'rb') as image_file:
                        tags = exifread.process_file(image_file)
                        # Create a dictionary to store all available metadata fields
                        metadata_entry = {}
                        for tag in tags:
                            tag_name = str(tag)
                            tag_value = str(tags[tag])
                            metadata_entry[tag_name] = tag_value
                        metadata.append(metadata_entry)
                except (IOError, FileNotFoundError, IsADirectoryError) as e:
                    print(f'Error reading metadata for {file}: \n{e}')
            else:
                print('Unsupported file format for this OS')
    return metadata


def get_media_type(path):
    try:
        kind = filetype.guess(path)
        mime = kind.mime
        media_type = mime.split('/')[0]
        return media_type
    except:
        return None


def case_insensitive_exists(path):
        # Get the directory containing the file
        directory, filename = os.path.split(path)
        # List all files in the directory
        files = os.listdir(directory)
        # Convert the target filename to lowercase
        filename_lower = filename.lower()
        # Check if a file with the same lowercase filename exists
        for file in files:
            if file.lower() == filename_lower:
                return directory + '/' + file
        return False


def add_trailing_slash(strng):
    z = strng[-1]
    if z == '\\' or z == '/':
        return strng
    else:
        return strng + '/'


def copyfile(fileobj, duplicate_check=True):
    desired_basename = fileobj.new_basename
    n = 2
    # check if file exist in destination & rename if so
    while True:
        pathlib.Path(fileobj.dest_dir + fileobj.problem_path + fileobj.new_rel_dir).mkdir(parents=True, exist_ok=True)
        existing_file_path = case_insensitive_exists(fileobj.new_abs_path)
        if existing_file_path:
            if duplicate_check and are_duplicates_OS_dependent(fileobj.abs_path, existing_file_path) and fileobj.problem_path != '/Duplicates/':
                print(f'{fileobj.abs_path} has been recognised as a duplicate of {existing_file_path}')
                fileobj.problem_path = '/Duplicates/'
            else:
                fileobj.new_basename = f'{desired_basename} ({n})'
                n += 1
        else:
            break
    # copy file
    print(f'Copying {fileobj.abs_path} to {fileobj.new_abs_path}')
    shutil.copy2(fileobj.abs_path, fileobj.new_abs_path)

    if fileobj.updated_creation_date:
        creation_date = fileobj.updated_creation_date
    else:
        creation_date = fileobj.creation_date
    set_creation_date(fileobj.new_abs_path, creation_date)

    if fileobj.update_meta_date:
        update_file_meta_date(fileobj)
    return fileobj.new_abs_path


def movefile(fileobj, duplicate_check=True):
    desired_basename = fileobj.new_basename
    n = 2
    # check if file exist in destination & rename if so
    while True:
        pathlib.Path(fileobj.dest_dir + fileobj.problem_path + fileobj.new_rel_dir).mkdir(parents=True, exist_ok=True)
        existing_file_path = case_insensitive_exists(fileobj.new_abs_path)
        if existing_file_path:
            if duplicate_check and are_duplicates_OS_dependent(fileobj.abs_path, existing_file_path) and fileobj.problem_path != '/Duplicates/':
                print(f'{fileobj.abs_path} has been recognised as a duplicate of {existing_file_path}')
                fileobj.problem_path = '/Duplicates/'
            else:
                fileobj.new_basename = f'{desired_basename} ({n})'
            n += 1
        else:
            break

    # First, determine if it's a cross-volume move
    same_volume = os.stat(fileobj.abs_path).st_dev == os.stat(os.path.dirname(fileobj.new_abs_path)).st_dev
    # Then move the file
    shutil.move(fileobj.abs_path, fileobj.new_abs_path)
    # Decide whether to reset creation date
    if fileobj.updated_creation_date:
        set_creation_date(fileobj.new_abs_path, fileobj.updated_creation_date)
    elif not same_volume:
        set_creation_date(fileobj.new_abs_path, fileobj.creation_date)

    if fileobj.update_meta_date:
        update_file_meta_date(fileobj)
    return fileobj.new_abs_path


def set_creation_date(file, newcdate):
    # set creation date
    filebytes = file.encode('utf_8')
    newcdate = newcdate.replace("-", ":")
    etcommand = b"-FileCreateDate=" + newcdate.encode('utf_8')
    with exiftool.ExifToolHelper() as et:
        et.execute(etcommand, filebytes)


def daysbetween(d1, d2):
    # earlier date goes first to return positive number
    try:
        d1 = datetime.strptime(d1, "%Y-%m-%d %H-%M-%S")
        d2 = datetime.strptime(d2, "%Y-%m-%d %H-%M-%S")
        secondsapart = (d2 - d1).total_seconds()
        daysapart = secondsapart / 86400
        return daysapart
    except:
        return 9999


def secondsbetween(d1, d2):
    # earlier date goes first
    d1 = d1.replace(':', '-')
    d2 = d2.replace(':', '-')
    d1 = datetime.strptime(d1, "%Y-%m-%d %H-%M-%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H-%M-%S")
    secondsapart = (d2 - d1).total_seconds()
    return secondsapart


def parentdirname(pathstr, levels=1):
    i = 0
    while i < levels:
        pathstr = os.path.split(pathstr)[0]
        i = i + 1
    dirname = os.path.split(pathstr)[1]
    dirname = dirname.replace("_1", "")
    return dirname


def analyse_date(date_input):
    # checks if string is a date and if so returns it in standard format
    try:
        # noinspection PyTypeChecker
        date_as_list = list(datefinder.find_dates(date_input, base_date=datetime(2000, 1, 1)))
        if len(date_as_list) != 1:
            # Found zero or multiple dates
            return False
        out_check_a = date_as_list[0]
        # noinspection PyTypeChecker
        out_check_b = list(datefinder.find_dates(date_input, base_date=datetime(2001, 2, 2)))[0]
        if (out_check_a.year != out_check_b.year) or (out_check_a.month != out_check_b.month):
            return False
        else:
            datewrongformat = str(out_check_a)
            daterightformat = datewrongformat.replace(":", "-")
            return daterightformat
    except ValueError:
        return False


def get_list_of_files(directory, folder_depth=None):
    all_files = []
    for root, dirs, files in os.walk(directory):
        depth = root[len(directory) + len(os.path.sep):].count(os.path.sep)
        if folder_depth is None or depth <= folder_depth:
            for file in files:
                all_files.append(os.path.normpath(os.path.join(root, file)))
    return all_files

def datelogic(
    fileobj,
    need_folderdate_match,
    cdate_beats_mdate,
    only_use_folderdate,
):
    """
    Decide which date to adopt, prioritizing folder/filename/meta/file date
    according to your existing logic. If known_year or known_month are provided,
    then we first discard any date fields that don't match that year/month.
    """

    # We'll create local variables for each date field so we can selectively
    # set them to False if they don't match known_year/month:
    cdate = fileobj.updated_creation_date  # This is the "file date" (creation/mod)
    mdate = fileobj.meta_date
    fdate = fileobj.filename_date
    flddate = fileobj.folder_date
    known_year = fileobj.known_year
    known_month = fileobj.known_month

    # A helper to check if a date string "YYYY-MM-DD HH-MM-SS" matches known year/month
    def matches_year_month(date_str):
        if not date_str or date_str is False:
            return False
        if known_year is None and known_month is None:
            return True  # not forcing any year/month, accept
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H-%M-%S")
        except ValueError:
            return False
        if known_year is not None and dt.year != known_year:
            return False
        if known_month is not None and dt.month != known_month:
            return False
        return True

    # Filter out date fields that do NOT match the forced year/month (if any)
    if not matches_year_month(cdate):
        cdate = False
    if not matches_year_month(mdate):
        mdate = False
    if not matches_year_month(fdate):
        fdate = False
    if not matches_year_month(flddate):
        flddate = False

    # Now the main logic:

    # First, see if we can unify folder_date and filename_date:
    filename_or_folder_date = None
    if flddate and fdate:
        # If they're within 32 days, prefer filename date, else folder date
        if abs(daysbetween(flddate, fdate)) <= 32:
            filename_or_folder_date = fdate
        else:
            filename_or_folder_date = flddate
    elif fdate:
        filename_or_folder_date = fdate
    else:
        filename_or_folder_date = flddate

    # If both folder_date and filename_date are False (meaning no match or none exist):
    if filename_or_folder_date is False:
        # If the user requires folderdate:
        if need_folderdate_match or only_use_folderdate:
            print(f"Failed to get folder date for {fileobj.abs_path}, - not allowed to proceed (need_folderdate_match=True)")
            return False
        else:
            # If meta_date is close to file date, pick whichever has priority
            days_mdate_to_cdate = daysbetween(mdate, cdate)
            if days_mdate_to_cdate < 0:        # cdate is earlier than mdate
                return cdate
            elif days_mdate_to_cdate < 62:    # mdate is less than 62 days before cdate
                    return mdate
            # mdate is more than 62 days before cdate
            elif known_year:
                if cdate_beats_mdate or not mdate:
                    return cdate
                else:
                    return mdate
            else:
                print(f"Failed to get folder date and metadata date for {fileobj.abs_path}")
                return False

    # If we have some folder/filename date:
    days_mdate_to_cdate = daysbetween(mdate, cdate)                         # If negative, cdate is earlier than mdate.
    days_c_to_f_or_f_date = daysbetween(cdate, filename_or_folder_date)     # If negative, filename_or_folder_date is earlier than cdate.
    days_m_to_f_or_f_date = daysbetween(mdate, filename_or_folder_date)     # If negative, filename_or_folder_date is earlier than mdate.

    if days_mdate_to_cdate < 0:
        cdate_beats_mdate = True

    if cdate_beats_mdate:
        # Check if cdate is close (within 62 days) to folder/filename date
        if abs(days_c_to_f_or_f_date) < 62:
            print(f"Using file date for {fileobj.abs_path} (which matches folder/filename date)")
            return cdate
        elif abs(days_m_to_f_or_f_date) < 62:
            print(f"Using metadata date for {fileobj.abs_path} (which matches folder/filename date)")
            return mdate
        else:
            # If we're forced to match folderdate but neither cdate nor mdate is close:
            if need_folderdate_match:
                print(f"Failed to get an accurate date for {fileobj.abs_path} because none agreed with folderdate")
                return False
            elif abs(days_mdate_to_cdate) < 62:
                print(f"Using file date for {fileobj.abs_path} (it does not match folder date, but is close to meta date)")
                return cdate
            else:
                print(f"Failed to get an accurate date for {fileobj.abs_path} because none agreed")
                return False
    else:
        # Meta date has priority
        if mdate and abs(days_m_to_f_or_f_date) < 62:
            print(f"Using metadata date for {fileobj.abs_path}, which matches folder/filename date")
            return mdate
        elif cdate and abs(days_c_to_f_or_f_date) < 62:
            print(f"Using file date for {fileobj.abs_path}, which matches folder/filename date")
            return cdate
        else:
            if need_folderdate_match:
                print(f"Failed to get an accurate date for {fileobj.abs_path} because none matched folderdate")
                return False
            elif abs(days_mdate_to_cdate) < 62:
                print(f"Using metadata date for {fileobj.abs_path} (it does not match folder date, but is close to file date)")
                return mdate
            else:
                print(f"Failed to get an accurate date for {fileobj.abs_path} because none of them agreed")
                return False



def processfile(
    abs_path,
    source_dir,
    dest_dir,
    gui_obj=None,
    rename=False,
    movefiles=False,
    need_folderdate_match=False,
    filedate_beats_metadadate=False,
    update_file_date=False,
    update_meta_date=False,
    only_use_folderdate=False,
    rename_folder=True,
    known_year=None,
    known_month=None
):
    if gui_obj and gui_obj.stop_event and gui_obj.stop_event.is_set():
        return

    print(f'Processing {abs_path}')

    supported_extensions = (
        '.jpg', '.jpeg', '.heic', '.mov',
        '.png', '.mp4', '.m4v', '.mpg', '.avi'
    )

    fileobj = filemanager.get_file(abs_path, source_dir, known_year=known_year, known_month=known_month)
    fileobj.dest_dir = dest_dir

    is_supported = fileobj.extension in supported_extensions

    # Determine fallback creation date
    fileobj.updated_creation_date = (
        fileobj.modified_date if daysbetween(fileobj.creation_date, fileobj.modified_date) < 0
        else fileobj.creation_date
    )

    if not is_supported:
        print(f"{abs_path} has an unsupported extension; placing in /Unsupported/")
        fileobj.problem_path = '/Unsupported/'
        fileobj.new_basename = fileobj.basename
        fileobj.new_rel_dir = fileobj.rel_dir
        finalfilepath = movefile(fileobj) if movefiles else copyfile(fileobj)
        if gui_obj:
            with gui_obj.files_processed_lock:
                gui_obj.files_processed += 1
        return  # Skip rest of processing

    # Now run your existing date logic
    fileobj.decided_date = datelogic(
        fileobj,
        need_folderdate_match,
        filedate_beats_metadadate,
        only_use_folderdate
    )

    # If no date found, place in "Couldn't Sort"
    if fileobj.decided_date is False:
        print("Couldn't sort " + abs_path + " due to failing to get an accurate date")
        fileobj.problem_path = '/Couldn\'t Sort/'
        fileobj.new_basename = fileobj.basename
        # We still preserve original subfolders
        fileobj.new_rel_dir = fileobj.rel_dir + '/'
    else:
        # Possibly update the file date
        if update_file_date:
            fileobj.updated_creation_date = fileobj.decided_date

        # Possibly overwrite metadata
        fileobj.update_meta_date = update_meta_date

        # Decide subfolder name
        if rename_folder and fileobj.decided_date:
            # For example: YYYY-MM
            fileobj.new_rel_dir = fileobj.decided_date[:7] + '/'
        else:
            # Keep original subfolder
            fileobj.new_rel_dir = fileobj.rel_dir + '/'

        # Rename file or not
        if rename and fileobj.decided_date:
            if fileobj.camera_model:
                fileobj.new_basename = f"{fileobj.decided_date} - {fileobj.camera_model}"
            else:
                fileobj.new_basename = fileobj.decided_date
        else:
            fileobj.new_basename = fileobj.basename

    if movefiles:
        finalfilepath = movefile(fileobj)
    else:
        finalfilepath = copyfile(fileobj)

    if gui_obj and finalfilepath:
        with gui_obj.files_processed_lock:
            gui_obj.files_processed += 1



def bulkprocess(
    source_dir,
    dest,
    gui_obj=None,
    rename_files=False,
    movefiles=False,
    need_folderdate_match=False,
    filedate_beats_metadadate=False,
    update_file_date=False,
    update_meta_date=False,
    only_use_folderdate=False,
    rename_folders=True,
    known_year=None,
    known_month=None
):
    """
      :param known_year:     int or None - only consider date fields that match this year
      :param known_month:    int or None - only consider date fields that match this month
    """
    t0 = time.time()
    listoffiles = get_list_of_files(source_dir)
    if gui_obj:
        gui_obj.total_files = len(listoffiles)

    # Start concurrency
    with concurrent.futures.ThreadPoolExecutor(8) as executor:
        futures = [
            executor.submit(
                processfile,
                abs_path,
                source_dir,
                dest,
                gui_obj,
                rename_files,
                movefiles,
                need_folderdate_match,
                filedate_beats_metadadate,
                update_file_date,
                update_meta_date,
                only_use_folderdate,
                rename_folders,
                known_year,
                known_month
            )
            for abs_path in listoffiles
        ]

        # Iterate over completed futures and print exceptions if any
        for future in concurrent.futures.as_completed(futures):
            try:
                # This will re-raise any exception that occurred in the worker
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")

            # Check for early stop signal
            if gui_obj and gui_obj.stop_event and gui_obj.stop_event.is_set():
                print('\nStarting shut down of ThreadPoolExecutor...\n')
                executor.shutdown(wait=False, cancel_futures=True)
                break

    t1 = time.time()
    totaltime = round(t1 - t0)

    if gui_obj and gui_obj.stop_event and gui_obj.stop_event.is_set():
        print('\nStopped by user after ' + str(totaltime) + ' seconds')
    else:
        print('\nFinished in ' + str(totaltime) + ' seconds')

    if gui_obj and gui_obj.finish_event:
        gui_obj.finish_event.set()  # Indicate that the process has completed



def fixcreationdate(path, source_dir):
    print ('Processing ' + path)
    try:
        fileobj = filemanager.get_file(path, source_dir)
        if daysbetween(fileobj.creation_date, fileobj.modified_date) < 0:
            print ('Fixing creation date from ' + fileobj.creation_date + ' to ' + fileobj.modified_date)
            fileobj.updated_creation_date = fileobj.modified_date
            set_creation_date(fileobj.abs_path, fileobj.updated_creation_date)
    except Exception as e:
        raise RuntimeError(f'Fixcreationdate failed with error: {str(e)}')



def bulkfixcreationdates(dir):
    t0 = time.time()
    print ('\nGetting list of files in ' + dir + '...')
    listoffiles = get_list_of_files(dir)
    print ('\nGot list of files')
    # n_threads = len(listoffiles)
    with concurrent.futures.ThreadPoolExecutor(48) as executor:
        _ = [executor.submit(fixcreationdate, path, dir) for path in listoffiles]
    t1 = time.time()
    totaltime = t1 -t0
    totaltime = round(totaltime)
    print ('\nFinished in ' + str(totaltime) + ' seconds')


#######################################################################################################################


# Constants
IMG_META_TAGS = [
    'EXIF:ExifImageWidth',
    'EXIF:ExifImageHeight',
    'EXIF:Make',
    'EXIF:Model',
    'EXIF:DateTimeOriginal',
    'EXIF:ExposureTime',
    'EXIF:FNumber',
    'EXIF:ISO',
    'EXIF:FocalLength',
    'EXIF:GPSLatitude',
    'EXIF:GPSLongitude'
]

VID_META_TAGS = [
    'QuickTime:CreateDate',
    'QuickTime:MediaCreateDate',
    'QuickTime:ContentCreateDate',
    'QuickTime:Duration',
    'QuickTime:ImageWidth',
    'QuickTime:ImageHeight'
]


def find_similar_fnames(input_path):
    directory = os.path.dirname(input_path)
    input_filename, input_extension = os.path.splitext(os.path.basename(input_path))
    matching_files = []
    # Create a regular expression pattern to match the format "filename (integer).extension"
    pattern = re.compile(rf'{re.escape(input_filename)}(\s*\(\d+\))?(\.jpg|\.jpeg|{re.escape(input_extension)})', re.IGNORECASE)
    for filename in os.listdir(directory):
        full_path = os.path.join(directory, filename)
        if os.path.isfile(full_path):
            match = pattern.fullmatch(filename)
            if match:
                matching_files.append(filename)
    return matching_files


def generate_unique_filename(base_filename, existing_filenames):
    if not existing_filenames:
        return base_filename
    filename, ext = os.path.splitext(base_filename)
    n = 2
    new_filename = base_filename
    while any(new_filename.lower() == name.lower() for name in existing_filenames):
        new_filename = f"{filename} ({n}){ext}"
        n += 1
    return new_filename


def convert_heic_to_jpeg(orig_file_path, jpeg_path, quality=90):
    register_heif_opener()
    try:
        dir = os.path.split(orig_file_path)[0]
        origfileobj = filemanager.get_file(orig_file_path, dir)
        with Image.open(orig_file_path) as img:  # Open the image file
            icc_profile = img.info.get('icc_profile')
            exif_data = img.getexif()
            # Save the image with appropriate parameters
            img.save(jpeg_path, format='JPEG', quality=quality, icc_profile=icc_profile, exif=exif_data)
        set_creation_date(jpeg_path, origfileobj.creation_date)
    except Exception as e:
        raise RuntimeError(f'Conversion failed with error: {str(e)}')


def compare_exif(fileobj1, fileobj2, filetype):
    metadata1 = fileobj1.metadata
    metadata2 = fileobj2.metadata
    if filetype == 'image':
        meta_tags = IMG_META_TAGS
    elif filetype == 'video':
        meta_tags = VID_META_TAGS
    else:
        print(f'File {fileobj1} is not an image or a video - can not get metadata')
        meta_tags = []
    missing_keys = set()
    for key in meta_tags:  # Use the central list of relevant metadata
        try:
            value1 = metadata1.get(key)
            value2 = metadata2.get(key)
        except Exception as e:
                print(f"An error occurred: {e}")
                return None
        if value1 is None and value2 is None:
            missing_keys.add(key)
        elif value1 is None:
            if key == 'EXIF:DateTimeOriginal':
                value1 = fileobj1.decided_date.replace("-", ":")
            else:
                return 'missing'
        elif value2 is None:
            if key == 'EXIF:DateTimeOriginal':
                value2 = fileobj2.decided_date.replace("-", ":")
            else:
                return 'missing'
        if is_numeric(value1) and is_numeric(value2):
            value1 = round(float(value1), 5)
            value2 = round(float(value2), 5)
        if value1 != value2:
            if (key == 'EXIF:ExifImageWidth' or key == 'EXIF:ExifImageHeight') and metadata1.get('EXIF:Orientation') != metadata2.get('EXIF:Orientation'):
                pass
            elif key == 'EXIF:DateTimeOriginal':
                if abs(secondsbetween(value1, value2)) <= 1:
                    print('Meta dates are very slightly different - assuming to be the same')
                    pass
                elif fileobj1.decided_date.replace("-", ":") == value2 or fileobj2.decided_date.replace("-", ":") == value1:
                    print('Decided date matches date taken')
                    pass
            else:
                return 'different'
    if len(missing_keys) == len(meta_tags):
        print("Both files are missing all metadata keys.")
        return 'all missing'
    return 'same'


def imgcomp(fileobj1, fileobj2):
    try:
        similarity = 1.0 - (fileobj1.image_hash - fileobj2.image_hash) / len(fileobj1.image_hash.hash) ** 2
        #print(f"Similarity between the images: {similarity:.1%}")
        return similarity
    except:
        return 0


def dedupe_image_files(file_list, source_dir, SIMILARITY_THRESHOLD=1):
    image_files = []
    for file_path in file_list:
        image_files.append(filemanager.get_file(file_path, source_dir))
    hash_file_dict = {}
    for image_file in image_files:
        hash_file_dict.setdefault(image_file.image_hash, []).append(image_file)
    files_to_delete = set()
    for group in hash_file_dict.values():
        if len(group) > 1:
            group.sort(key=lambda image_file: image_file.no_of_tags, reverse=True)
            for i in range(len(group) - 1):
                for j in range(i + 1, len(group)):
                    similarity = imgcomp(group[i], group[j])
                    print(f'Comparing hashes of {group[i].abs_path} and {group[j].abs_path}: Similarity = {similarity:.1%}')
                    if similarity > SIMILARITY_THRESHOLD:
                        if group[j].no_of_tags > group[i].no_of_tags:
                            files_to_delete.add(group[i].abs_path)
                        else:
                            files_to_delete.add(group[j].abs_path)
    files_to_keep = [image_file.abs_path for image_file in image_files if image_file.abs_path not in files_to_delete]
    for file_to_delete in files_to_delete:
        print(f'Deleting file {file_to_delete}')
        os.remove(file_to_delete)
    update_filenames(files_to_keep)


def update_filenames(file_paths):
    file_dict = {}
    # Create a dictionary to store the base filenames and their corresponding indices
    for file_path in file_paths:
        base_name, extension = os.path.splitext(os.path.basename(file_path))
        match = re.match(r'^(.*?) \((\d+)\)$', base_name)
        if match:
            base_name = match.group(1)
            index = int(match.group(2))
        else:
            index = 1
        if base_name in file_dict:
            file_dict[base_name].append((index, extension, file_path))
        else:
            file_dict[base_name] = [(index, extension, file_path)]
    # Rename files as necessary
    for base_name, file_list in file_dict.items():
        file_list.sort(key=lambda x: x[0])
        for i, (_, extension, old_path) in enumerate(file_list):
            new_index = i + 1
            new_base_name = base_name if new_index == 1 else f"{base_name} ({new_index})"
            new_path = os.path.join(os.path.dirname(old_path), f"{new_base_name}{extension}")
            if old_path != new_path:
                os.rename(old_path, new_path)
                print(f"Renamed {old_path} to {new_path}")


def are_meta_duplicates(fileobj1, fileobj2):
    filetype = get_media_type(fileobj1.abs_path)
    if not filetype:
        return False
    meta_comp_result = compare_exif(fileobj1, fileobj2, filetype)  # Compare metadata
    if meta_comp_result in ['same']:
        return True
    elif meta_comp_result in ['all missing']:
        return 'unknown'
    else:
        return False


def are_hash_duplicates(fileobj1, fileobj2, SIMILARITY_THRESHOLD=0.99):
    if imgcomp(fileobj1, fileobj2) >= SIMILARITY_THRESHOLD:
        return True
    else:
        return False


def are_duplicates_OS_dependent(path1, path2):
    if exiftool_supported:
        result = are_meta_duplicates(filemanager.get_file(path1), filemanager.get_file(path2))
        if result in ['unknown']:
            print('Trying hash dupliate comparison instead')
            return are_hash_duplicates(path1, path2)
        else:
            return result
    else:
        return are_hash_duplicates(path1, path2)

def check_for_matching_jpeg(heic_path, existing_jpeg_names):    #check if jpeg with the same name and metadata exits
    dir = os.path.dirname(heic_path)
    for jpeg_filename in existing_jpeg_names:
        jpeg_path = os.path.join(dir, jpeg_filename)
        meta_comp_result = compare_exif(heic_path, filemanager.get_file(jpeg_path,), 'image')  # Compare metadata
        if meta_comp_result in ['same']:
            print(f'A matching JPEG for {heic_path} already exists: {jpeg_filename}')
            return True
    return False


def smart_heic_to_jpeg(heic_path, source_dir, QUALITY=90):
    print(f'Smart conversion  of {heic_path}')
    if heic_path.lower().endswith('.heic'):
        dir = os.path.dirname(heic_path)
        fileobj = filemanager.get_file(heic_path, source_dir, True)
        jpeg_path = os.path.splitext(heic_path)[0] + '.jpg'
        existing_jpeg_names = find_similar_fnames(jpeg_path)
        if existing_jpeg_names:
            print(f'There are already some jpegs similar to {heic_path}')
            if check_for_matching_jpeg(heic_path, existing_jpeg_names):  # if there's a matching jpeg (using metadata)
                print(f'Found a jpeg match for {heic_path} with the same name. Skipping.')
                return
            else:
                jpeg_name = os.path.basename(jpeg_path)
                jpeg_name = generate_unique_filename(jpeg_name, existing_jpeg_names)
                jpeg_path = os.path.join(dir, jpeg_name)
                convert_heic_to_jpeg(heic_path, jpeg_path, QUALITY)
                dupe_path_list = [os.path.join(dir, file) for file in existing_jpeg_names] + [jpeg_path]
            dedupe_image_files(dupe_path_list, source_dir)
        else:
            print(f'No existing jpegs similar to {heic_path}. Creating jpeg.')
            convert_heic_to_jpeg(heic_path, jpeg_path, QUALITY)


def bulk_convert_heic(dir_path, QUALITY=90):
    t0 = time.time()
    file_list = []
    for root, dirs, files in os.walk(dir_path):
        files = [f for f in files if f.lower().endswith('.heic')]
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_list.append(file_path)
    with concurrent.futures.ThreadPoolExecutor(12) as executor:
        _ = [executor.submit(smart_heic_to_jpeg, abs_path, dir_path, QUALITY) for abs_path in file_list]
    t1 = time.time()
    totaltime = t1 - t0
    totaltime = round(totaltime)
    print('\nFinished in ' + str(totaltime) + ' seconds')


def is_numeric(input_str):
    try:
        float(input_str)
        return True
    except:
        return False


def copy_to_date_dir_format(source_dir, destination):
    bulkprocess(source_dir, destination, only_use_folderdate=True)


def sort_by_exif_quality(abs_path, source_dir, dest_dir):
    print ('Processing ' + abs_path)
    supported_extensions = ('.jpg', '.jpeg', '.heic', '.mov', '.png', '.mp4', '.m4v', '.mpg')
    fileobj = filemanager.get_file(abs_path, source_dir)
    fileobj.new_basename = fileobj.basename
    if fileobj.extension not in supported_extensions:
        print(abs_path + ' does not have an accepted extension; `skipping...')
    else:
        filetype = get_media_type(abs_path)
        all_meta = get_metadata([abs_path])[0]
        if filetype == 'image':
            meta_tags = IMG_META_TAGS
        elif filetype == 'video':
            meta_tags = VID_META_TAGS
        else:
            print(f'File {abs_path} is not an image or a video - can not get metadata')
            meta_tags = []
        missing_keys = set()
        for key in meta_tags:  # Use the central list of relevant metadata
            value = all_meta.get(key)
            if value is None:
                missing_keys.add(key)
        if len(missing_keys) == len(meta_tags):
            print("Missing all metadata keys.")
            fileobj.dest_dir = dest_dir + '/Missing_all_meta/'
            fileobj.new_rel_dir = fileobj.rel_dir + '/'
            finalfilepath = copyfile(fileobj)
            if finalfilepath is not False:
                set_creation_date(finalfilepath, fileobj.creation_date)
        elif len(missing_keys) == 0:
            print("Has all metadata keys.")
            fileobj.dest_dir = dest_dir + '/Full_metadata/'
            fileobj.new_rel_dir = fileobj.rel_dir + '/'
            finalfilepath = copyfile(fileobj)
            if finalfilepath is not False:
                set_creation_date(finalfilepath, fileobj.creation_date)
        else:
            print(f"Missing {len(missing_keys)} metadata keys: {missing_keys}")
            fileobj.dest_dir = dest_dir + '/Missing_some_meta/'
            fileobj.new_rel_dir = fileobj.rel_dir + '/'
            finalfilepath = copyfile(fileobj)
            if finalfilepath is not False:
                set_creation_date(finalfilepath, fileobj.creation_date)

def bulkprocess_sort_by_exif_quality(source_dir, dest):
    t0 = time.time()
    listoffiles = get_list_of_files(source_dir)
    with concurrent.futures.ThreadPoolExecutor(16) as executor:
        _ = [executor.submit(sort_by_exif_quality, abs_path, source_dir, dest) for abs_path in listoffiles]
    t1 = time.time()
    totaltime = t1-t0
    totaltime = round(totaltime)
    print ('\nFinished in ' + str(totaltime) + ' seconds')

def edit_metadata(image_path, metadata_changes):
    if exiftool_supported:
        print('Starting metadata edit...')
        with exiftool.ExifToolHelper() as et:
            try:
                et.execute("-overwrite_original", *[f"-{k}={v}" for k, v in metadata_changes.items()], image_path)
                print("Metadata edited successfully.")
            except Exception as e:
                print(f"An error occurred while editing metadata: {e}")
    else:
        print('Exiftool needs to be installed to do that')

def update_file_meta_date(fileobj):
    new_date = fileobj.decided_date.replace("-", ":")
    metadata_changes = {}
    if fileobj.media_type == 'image':
        metadata_changes['DateTimeOriginal'] = new_date
    elif fileobj.media_type == 'video':
        metadata_changes['CreateDate'] = new_date
        metadata_changes['MediaCreateDate'] = new_date
    else:
        print('Can\'t change exif date')
        return  # Return if media type is not recognized
    edit_metadata(fileobj.new_abs_path, metadata_changes)

def change_extension_case(folder_path, case='l'):
    # Get a list of all files in the folder
    files = os.listdir(folder_path)
    # Convert case to either uppercase or lowercase
    case = case.lower()
    # Traverse through the directory tree
    for root, directories, files in os.walk(folder_path):
        # Iterate over each file in the current directory
        for file_name in files:
            # Split the file name and extension
            base_name, old_extension = os.path.splitext(file_name)
            # Construct the new file name with the new extension
            new_extension_formatted = old_extension.upper() if case == 'u' else old_extension.lower()
            new_file_name = f"{base_name}{new_extension_formatted}"
            if new_file_name != file_name:
                # Rename the file
                print(f'Renaming {file_name} to {new_file_name}')
                os.rename(os.path.join(root, file_name), os.path.join(root, new_file_name))

def mirror_cdate_to_video_files(source_dir, dest_dir, folder_depth=1):
    # Written to batch copy file creation metadata to the new optimised versions of video files
    file_manager = FileManager()
    dest_files = get_list_of_files(dest_dir, folder_depth)
    for dest_file in dest_files:
        dest_fileobj = file_manager.get_file(dest_file, dest_dir, is_in_dest=True)
        if dest_fileobj.media_type == 'video':
            source_file = os.path.join(source_dir, dest_fileobj.rel_path)
            if os.path.exists(source_file):
                source_fileobj = file_manager.get_file(source_file, source_dir)
                set_creation_date(dest_file, source_fileobj.creation_date)
                dest_fileobj.decided_date = source_fileobj.creation_date
                update_file_meta_date(dest_fileobj)
        else:
            print(f"File {dest_fileobj.rel_path} does not exist in source directory.")


filemanager = FileManager()
if __name__ == "__main__":
    print('###############START###############')
    print('\n')
    #Test things

    print('All done!')


#   TODO: Add buttons to GUI for more useful functions.
#   TODO: Track files that don't finish processing successfully.
