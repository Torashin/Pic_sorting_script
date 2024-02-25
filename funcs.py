
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

    def add_file(self, abs_path, source_dir=None, is_in_dest=False):
        if abs_path not in self.file_objects_dict:
            self.file_objects_dict[abs_path] = FileObject(abs_path, source_dir, is_in_dest)

    def get_file(self, abs_path, source_dir=None, is_in_dest=False):
        if abs_path not in self.file_objects_dict:
            print(f"Creating FileObject for {abs_path}")
            self.add_file(abs_path, source_dir, is_in_dest)
        return self.file_objects_dict.get(abs_path)

class FileObject:
    def __init__(self, abs_path, source_dir=None, is_in_dest=False):
        self.source_dir = source_dir
        self.abs_path = abs_path
        self.abs_dir, self.filename = os.path.split(abs_path)
        self.basename, extension = os.path.splitext(self.filename)
        self.extension = extension.lower()
        self._rel_dir = None
        self._media_type = None
        self._creation_date = None
        self.updated_creation_date = None
        self._modified_date = None
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
        self.is_in_dest = is_in_dest

        if is_in_dest:
            self.dest_dir = source_dir
        else:
            self.dest_dir = None


    @property
    def rel_dir(self):
        if self._rel_dir is None:
            self._rel_dir = os.path.relpath(self.abs_dir, self.source_dir)
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
            self._metadata = get_metadata(self.abs_path)
        return self._metadata

    @property
    def new_abs_path(self):
        return str(self.dest_dir + self.problem_path + self.new_rel_dir + self.new_basename + self.extension)

    @property
    def no_of_ags(self):
        return len(self.metadata)

    @property
    def image_hash(self):
        if not self._image_hash:
            self._image_hash = imagehash.average_hash(self.abs_path)
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
            if duplicate_check and fileobj.problem_path != '/Duplicates/' and are_duplicates_OS_dependent(fileobj, filemanager.get_file(existing_file_path, fileobj.dest_dir, is_in_dest=True)):
                print(
                    f'{fileobj.abs_path} has been recognised as a duplicate of {existing_file_path}')
                fileobj.problem_path = '/Duplicates/'
            else:
                fileobj.new_basename = f'{desired_basename} ({n})'
            n += 1
        else:
            break
    # move file
    shutil.move(fileobj.abs_path, fileobj.new_abs_path)
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
    # earlier date goes first
    d1 = datetime.strptime(d1, "%Y-%m-%d %H-%M-%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H-%M-%S")
    secondsapart = (d2 - d1).total_seconds()
    daysapart = secondsapart / 86400
    return daysapart


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
        date_as_list = list(datefinder.find_dates(date_input, base_date=datetime(2000, 1, 15)))
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


def get_list_of_files(directory):
    allFiles = list()
    allFiles = list()
    for dirpath ,_ ,filenames in os.walk(directory):
        for f in filenames:
            allFiles.append(os.path.abspath(os.path.join(dirpath, f)))
    return allFiles


def datelogic(fileobj, need_folderdate_match, filedate_beats_metadadata, only_use_folderdate):
    if fileobj.folder_date == False:
        if need_folderdate_match or only_use_folderdate:
            print('Failed to get folder date for ' + fileobj.abs_path + ', - not allowed to proceed (need_folderdate_match=True)')
            return False
        elif fileobj.meta_date:
            # print ('Metadata date for ' + file + ' = ' + metadate)
            if abs(daysbetween(fileobj.updated_creation_date, fileobj.meta_date)) < 60:
                if filedate_beats_metadadata:
                    print('Using file date for ' + fileobj.abs_path + ', which matches folder date')
                    return fileobj.updated_creation_date
                else:
                    print('Using metadata date for ' + fileobj.abs_path + ', which matches folder date')
                    return fileobj.meta_date
            else:
                print ('Failed to get folder date for ' + fileobj.abs_path + ', and metadata and file dates don\'t match')
                return False
        else:
            print ('Failed to get folder date and metadata date for ' + fileobj.abs_path)
            return False
    elif only_use_folderdate:
        return  fileobj.folder_date
    elif filedate_beats_metadadata:
        if abs(daysbetween(fileobj.updated_creation_date, fileobj.folder_date)) < 60:
            print('Using file date for ' + fileobj.abs_path + ', which matches folder date')
            return fileobj.updated_creation_date
        elif fileobj.meta_date and abs(daysbetween(fileobj.meta_date, fileobj.folder_date)) < 60:
            print('Using metadata date for ' + fileobj.abs_path + ', which matches folder date')
            return fileobj.meta_date
        elif need_folderdate_match:
            print('Failed to get an accurate date for ' + fileobj.abs_path + ' because none of them agreed with folderdate')
            return False
        elif fileobj.meta_date and abs(daysbetween(fileobj.updated_creation_date, fileobj.meta_date)) < 60:
            print('Using file date for ' + fileobj.abs_path + ', but it does not match folder date')
            return fileobj.updated_creation_date
        else:
            print('Failed to get an accurate date for ' + fileobj.abs_path + ' because none of them agreed')
            return False
    else:
        if fileobj.meta_date and abs(daysbetween(fileobj.meta_date, fileobj.folder_date)) < 60:
            print('Using metadata date for ' + fileobj.abs_path + ', which matches folder date')
            return fileobj.meta_date
        elif abs(daysbetween(fileobj.updated_creation_date, fileobj.folder_date)) < 60:
            print('Using file date for ' + fileobj.abs_path + ', which matches folder date')
            return fileobj.updated_creation_date
        elif need_folderdate_match:
            print('Failed to get an accurate date for ' + fileobj.abs_path + ' because none of them agreed with folderdate')
            return False
        elif fileobj.meta_date and abs(daysbetween(fileobj.updated_creation_date, fileobj.meta_date)) < 60:
            print('Using metadata date for ' + fileobj.abs_path + ', but it does not match folder date')
            return fileobj.meta_date
        else:
            print('Failed to get an accurate date for ' + fileobj.abs_path + ' because none of them agreed')
            return False


filemanager = FileManager()


def processfile(abs_path, source_dir, dest_dir, gui_obj=None, rename=False, movefiles=False, need_folderdate_match=False, filedate_beats_metadadate=False, update_file_date=False, update_meta_date=False, only_use_folderdate = False):
    print ('Processing ' + abs_path)
    #supported_extensions = ('.pdf', '.cr2', '.mov', '.png', '.jpg', '.jpeg','.mpg', '.3gp', '.bmp', '.avi', '.wmv', '.xmp', '.mdi', '.tif', '.psf', '.xlsx', '.zip', '.doc', '.gif', '.pps', '.mpe', '.flv', '.asf', '.xls', '.psd', '.m2ts', '.heic', '.mp4', '.m4v')
    supported_extensions = ('.jpg', '.jpeg', '.heic', '.mov', '.png', '.mp4', '.m4v', '.mpg')
    fileobj = filemanager.get_file(abs_path, source_dir)
    fileobj.dest_dir = dest_dir
    if fileobj.extension not in supported_extensions:
        print(abs_path + ' does not have an accepted extension; `skipping...')
    else:
        if daysbetween(fileobj.creation_date, fileobj.modified_date) < 0:
            change_creation_date = True
            fileobj.updated_creation_date = fileobj.modified_date
        else:
            change_creation_date = False
            fileobj.updated_creation_date = fileobj.creation_date
        fileobj.decided_date = datelogic(fileobj, need_folderdate_match, filedate_beats_metadadate, only_use_folderdate)
        if fileobj.decided_date == False:
            print ('Couldn\'t sort ' + abs_path + ' due to failing to get an accurate date')
            fileobj.problem_path = '/Couldn\'t Sort/'
            fileobj.new_basename = fileobj.basename
            fileobj.new_rel_dir = fileobj.rel_dir + '/'
        else:
            if update_file_date:
                fileobj.updated_creation_date = fileobj.decided_date
            if update_meta_date:
                fileobj.update_meta_date = True
            fileobj.new_rel_dir = fileobj.decided_date[:7] + '/'
            if rename:
                if fileobj.camera_model == '':
                    fileobj.new_basename = fileobj.decided_date
                else:
                    fileobj.new_basename = f'{fileobj.decided_date} - {fileobj.camera_model}'
            else:
                fileobj.new_basename = fileobj.basename
        if movefiles:
            finalfilepath = movefile(fileobj)
            if finalfilepath is not False and change_creation_date:
                set_creation_date(finalfilepath, fileobj.updated_creation_date)
        else:
            finalfilepath = copyfile(fileobj)
            if finalfilepath is not False:
                set_creation_date(finalfilepath, fileobj.updated_creation_date)
                # Update files_processed count with lock
    if gui_obj:
        with gui_obj.files_processed_lock:
            gui_obj.files_processed += 1
    #print ('Finished moving file to ' + newfilepath)


def bulkprocess(source_dir, dest, gui_obj=None, rename=False, movefiles=False, need_folderdate_match=False, filedate_beats_metadadate=False, update_file_date=False, update_meta_date=False, only_use_folderdate=False):
    t0 = time.time()
    listoffiles = get_list_of_files(source_dir)
    if gui_obj:
        gui_obj.total_files = len(listoffiles)
    with concurrent.futures.ThreadPoolExecutor(16) as executor:
        _ = [executor.submit(processfile, abs_path, source_dir, dest, gui_obj, rename, movefiles, need_folderdate_match, filedate_beats_metadadate, update_file_date, update_meta_date, only_use_folderdate) for abs_path in listoffiles]
    t1 = time.time()
    totaltime = t1-t0
    totaltime = round(totaltime)
    print ('\nFinished in ' + str(totaltime) + ' seconds')


def fixcreationdate(path, source_dir):
    print ('Processing ' + path)
    fileobj = filemanager.get_file(path, source_dir)
    if daysbetween(fileobj.creation_date, fileobj.modified_date) < 0:
        print ('Fixing creation date from ' + fileobj.creation_date + ' to ' + fileobj.modified_date)
        fileobj.updated_creation_date = fileobj.modified_date
        set_creation_date(fileobj.abs_path, fileobj.updated_creation_date)


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
        img = Image.open(orig_file_path)
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
        value1 = metadata1.get(key)
        value2 = metadata2.get(key)
        if value1 is None and value2 is None:
            missing_keys.add(key)
        elif value1 is None or value2 is None:
            return 'missing'
        if is_numeric(value1) and is_numeric(value2):
            value1 = round(float(value1), 5)
            value2 = round(float(value2), 5)
        if value1 != value2:
            if (key == 'EXIF:ExifImageWidth' or key == 'EXIF:ExifImageHeight') and metadata1.get('EXIF:Orientation') != metadata2.get('EXIF:Orientation'):
                pass
            if key == 'EXIF:DateTimeOriginal' and abs(secondsbetween(value1, value2)) <= 1:
                print('Meta dates are very slightly different - assuming to be the same')
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
        result = are_meta_duplicates(path1, path2)
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

def edit_metadata(image_path, tag, new_value):
    if exiftool_supported:
        with exiftool.ExifToolHelper() as et:
            metadata = {tag: new_value}
            try:
                et.execute("-overwrite_original", *[f"-{k}={v}" for k, v in metadata.items()], image_path)
                print("Metadata edited successfully.")
            except Exception as e:
                print(f"An error occurred while editing metadata: {e}")
    else:
        print('Exiftool needs to be installed to do that')

def update_file_meta_date(fileobj):
    new_date = fileobj.decided_date.replace("-", ":")
    if fileobj.media_type == 'image':
        edit_metadata(fileobj.new_abs_path, 'DateTimeOriginal', new_date)
    elif fileobj.media_type == 'video':
        edit_metadata(fileobj.new_abs_path, 'CreateDate', new_date)
    else:
        print('Can\'t change exif date')


if __name__ == "__main__":
    print('###############START###############')
    print('\n')
    #Test things#
    print(get_metadata([r'/Users/James/Desktop/test/2024-02/IMG_0893 21.27.16.JPG']))
    #edit_metadata(r'/Users/James/Desktop/IMG-20211017-WA0000.jpg', "DateTimeOriginal", "2024:02:19 12:00:00")
    #print(get_metadata([r'/Users/James/Desktop/dest/2024-02/IMG_0893 21.27.16.JPG']))
    print('All done!')


#   TODO: Add buttons to GUI for more useful functions.
#   TODO: Stop getting metadata again when duplicate checking.
#   TODO: Fix duplicate checking when using option to update metadata date.
#   TODO: Function to change file extensions to upper or lower case.
