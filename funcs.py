
import os
import shutil
import time
from datetime import datetime
import exiftool                     # exiftool.exe needs installing from exiftool.org, along with the PyExifTool package
from dateutil.parser import parse
import pathlib
import concurrent.futures


defaultsourcedir = r'C:\Users\james\PycharmProjects\Source'
defaultdestdir = r'C:\Users\james\PycharmProjects\Dest'


print ('###############START###############')
print ('\n')


def printmetadata(files):
    with exiftool.ExifToolHelper() as et:
        metadata = et.get_metadata(files)
        print(metadata)

# printmetadata(r'C:\Users\james\PycharmProjects\Source\2002-09\2002-09-19 18-38-34 - Dimage 2300.JPG')

def checkslash(strng):
    z = strng[-1]
    if z == '\\' or z == '/':
        return strng
    else:
        return strng + '/'


def copyfile(src_path, dst_path):
    split_input_path = os.path.split(src_path)
    orig_fname = split_input_path[1]
    src_folder = split_input_path[0]  + '/'
    split_output_path = os.path.split(dst_path)
    new_desired_fname = split_output_path[1]
    dst_folder = split_output_path[0] + '/'
    splitname_orig = os.path.splitext(orig_fname)
    splitname_new = os.path.splitext(new_desired_fname)
    extension = splitname_orig[1]
    new_fname = splitname_new[0] + extension
    pathlib.Path(dst_folder).mkdir(parents=True, exist_ok=True)
    n = 2
    # check if file exist in destination & rename if so
    while os.path.exists(dst_folder + new_fname):
        # Split name and extension
        splitname_new = os.path.splitext(new_desired_fname)
        only_name = splitname_new[0]
        # Adding the new name
        new_fname = only_name + ' (' + str(n) + ')' + extension
        n = n + 1
    # copy file
    shutil.copy2(src_folder + orig_fname, dst_folder + new_fname)
    return dst_folder + new_fname


def movefile(src_path, dst_path):
    split_input_path = os.path.split(src_path)
    orig_fname = split_input_path[1]
    src_folder = split_input_path[0]  + '/'
    split_output_path = os.path.split(dst_path)
    new_desired_fname = split_output_path[1]
    dst_folder = split_output_path[0] + '/'
    splitname_orig = os.path.splitext(orig_fname)
    splitname_new = os.path.splitext(new_desired_fname)
    extension = splitname_orig[1]
    new_fname = splitname_new[0] + extension
    pathlib.Path(dst_folder).mkdir(parents=True, exist_ok=True)
    n = 2
    # check if file exist in destination & rename if so
    while os.path.exists(dst_folder + new_fname):
        # Split name and extension
        splitname_new = os.path.splitext(new_desired_fname)
        only_name = splitname_new[0]
        # Adding the new name
        new_fname = only_name + ' (' + str(n) + ')' + extension
        n = n + 1
    # copy file
    shutil.move(src_folder + orig_fname, dst_folder + new_fname)
    return dst_folder + new_fname


def getcdate(path):
    # get creation time in seconds
    ct_sec = os.path.getctime(path)
    # convert to date (in wrong format)
    ct_stamp_wrong = time.ctime(ct_sec)
    # Using the timestamp string to create a
    # time object/structure
    ct_obj = time.strptime(ct_stamp_wrong)
    # Transforming the time object to a timestamp
    # of ISO 8601 format
    ct_stamp = time.strftime("%Y-%m-%d %H-%M-%S", ct_obj)
    return ct_stamp


def getmdate(path):
    # get modification time in seconds
    mt_sec = os.path.getmtime(path)
    # convert to date (in wrong format)
    mt_stamp_wrong = time.ctime(mt_sec)
    # Using the timestamp string to create a
    # time object/structure
    mt_obj = time.strptime(mt_stamp_wrong)
    # Transforming the time object to a timestamp
    # of ISO 8601 format
    mt_stamp = time.strftime("%Y-%m-%d %H-%M-%S", mt_obj)
    return mt_stamp


def gettdate(file):
    # get date taken from metadata
    with exiftool.ExifTool() as et:
        try:
            tagdata = et.get_tag("DateTimeOriginal", file)
        except:
            try:
                tagdata = et.get_tag("QuickTime:MediaCreateDate", file)
            except:
                tagdata = False
        # if tagdata is None:
        # tagdata = et.get_tag("QuickTime:MediaCreateDate", file)
        if tagdata is not False:
            tagdata = tagdata.replace(":", "-")
        # else:
        # tagdata = False
        return tagdata


def getcameramodel(file):
    # get camera model from metadata
    with exiftool.ExifTool() as et:
        tagdata = et.get_tag("Model", file)
        return tagdata


def setcdate(file, newcdate):
    # set creation date
    filebytes = file.encode('utf_8')
    newcdate = newcdate.replace("-", ":")
    etcommand = b"-FileCreateDate=" + newcdate.encode('utf_8')
    with exiftool.ExifTool() as et:
        et.execute(etcommand, filebytes)


def daysbetween(d1, d2):
    # earlier date goes first
    d1 = datetime.strptime(d1, "%Y-%m-%d %H-%M-%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H-%M-%S")
    secondsapart = (d2 - d1).total_seconds()
    daysapart = secondsapart / 86400
    return daysapart


def parentdirname(pathstr, levels=1):
    i = 0
    while i < levels:
        pathstr = os.path.split(pathstr)[0]
        i = i + 1
    dirname = os.path.split(pathstr)[1]
    dirname = dirname.replace("_1", "")
    return dirname


def analysedate(date_input):
    # checks if string is a date and if so returns it in standard format
    try:
        out_check_a = parse(date_input, default=datetime(2000, 1, 15))
        out_check_b = parse(date_input, default=datetime(2001, 2, 2))
        if (out_check_a.year != out_check_b.year) or (out_check_a.month != out_check_b.month):
            return False
        else:
            datewrongformat = str(out_check_a)
            daterightformat = datewrongformat.replace(":", "-")
            return daterightformat
    except ValueError:
        return False


def getListOfFiles(directory):
    allFiles = list()
    for dirpath ,_ ,filenames in os.walk(directory):
        for f in filenames:
            allFiles.append(os.path.abspath(os.path.join(dirpath, f)))
    return allFiles


def datelogic(file, filedate, folderdate):
    # print ('file date = ' + filedate)
    # try:
    # print ('folder date = ' + folderdate)
    # except:
    # print ('folder date = False')
    if folderdate == False:
        try:
            metadate = gettdate(file)
            print ('metadata date for ' + file + ' = ' + metadate)
            if abs(daysbetween(filedate, metadate)) < 60:
                print ('using file date for ' + file)
                return filedate
            else:
                print ('Failed to get folder date for ' + file + ', and metadata and file dates don\'t match')
                return False
        except:
            print ('Failed to get folder date and metadata date for ' + file)
            return False

    elif abs(daysbetween(filedate, folderdate)) < 60:
        print ('using file date for ' + file + ', which matches folder date')
        return filedate
    metadate = gettdate(file)
    print ('metadata date = ')
    print (metadate)
    if metadate == False:
        print ('Failed to get metadata date for ' + file + ', and folder and file dates don\'t match')
        return False
    else:
        metadate = str(metadate)[:19]
        print ('metadata date = ' + metadate)
        if abs(daysbetween(filedate, metadate)) < 60:
            print ('using file date for ' + file + ', but folder date doesn\'t match')
            return filedate
        elif abs(daysbetween(folderdate, metadate)) < 60:
            print ('using metadata date for ' + file)
            return metadate
        else:
            print ('Failed to get an accurate date for ' + file + ' because none of them agreed')
            return False


def processfile(file, dir, dest):
    print ('Processing ' + file)
    extension = os.path.splitext(file)[1]
    if extension.lower() == '.pdf' or extension.lower() == '.cr2' or extension.lower() == '.mov' or extension.lower() == '.png' or extension.lower() == '.jpg' or extension.lower() == '.mpg' or extension.lower() == '.3gp' or extension.lower() == '.bmp' or extension.lower() == '.avi' or extension.lower() == '.wmv' or extension.lower() == '.xmp' or extension.lower() == '.mdi' or extension.lower() == '.tif' or extension.lower() == '.psf' or extension.lower() == '.xlsx' or extension.lower() == '.zip' or extension.lower() == '.doc' or extension.lower() == '.gif' or extension.lower() == '.pps' or extension.lower() == '.mpe' or extension.lower() == '.flv' or extension.lower() == '.asf' or extension.lower() == '.xls' or extension.lower() == '.psd' or extension.lower() == '.m2ts':
        n = 1
        while analysedate(parentdirname(file, n)) == False:
            if n == 4:
                folderdate = False
                break
            else:
                n = n + 1
        folderdate = analysedate(parentdirname(file, n))
        creationdate = getcdate(file)
        modifieddate = getmdate(file)
        extension = os.path.splitext(file)[1]
        try:
            cameramodel = getcameramodel(file)
        except:
            cameramodel = ''
            # print ('can\'t get camera model for ' + file)
        if daysbetween(creationdate, modifieddate) < 0:
            change_creation_date = True
            creationdate = modifieddate
        else:
            change_creation_date = False
        decideddate = datelogic(file, creationdate, folderdate)
        if decideddate == False:
            print ('had to skip ' + file + ' due to failing to get an accurate date')
            # continue
            problempath = os.path.split(file)[0]
            problempath = problempath.replace(dir, "")
            problempath = 'Couldn\'t Sort/' + problempath
            newfilename = os.path.split(file)[1]
            newfilepath = checkslash(dest) + problempath + '/' + newfilename
        else:
            newfoldername = decideddate[:7] + '/'
            try:
                newfilename = decideddate + " - " + cameramodel
            except:
                newfilename = decideddate
            newfilepath = checkslash(dest) + newfoldername + newfilename
        finalfilepath = movefile(file, newfilepath)
        if change_creation_date == True:
            setcdate(finalfilepath, modifieddate)
        print ('Finished moving file to ' + newfilepath)
    else:
        print (file + ' does not have an accepted extension; skipping...')

# processfile (r'C:\Users\james\PycharmProjects\Source\2003-02\2003-02-16 08-55-24 - .JPG', r'C:\Users\james\PycharmProjects\Source\2003-02', r'C:\Users\james\PycharmProjects\Dest')

def bulkprocess(dir, dest):
    t0 = time.time()
    listoffiles = getListOfFiles(dir)
    with concurrent.futures.ThreadPoolExecutor(48) as executor:
        _ = [executor.submit(processfile, file, dir, dest) for file in listoffiles]
    t1 = time.time()
    totaltime = t1-t0
    totaltime = round(totaltime)
    print ('\nFinished in ' + str(totaltime) + ' seconds')

# bulkprocess (r'C:\Users\james\PycharmProjects\Source', r'C:\Users\james\PycharmProjects\Dest')


def fixcreationdate(file):
    print ('\nProcessing ' + file)
    creationdate = getcdate(file)
    modifieddate = getmdate(file)
    if daysbetween(creationdate, modifieddate) < 0:
        print ('Fixing creation date from ' + creationdate + ' to ' + modifieddate)
        setcdate(file, modifieddate)
    # print ('Finished!')


def seriallyfixcreationdates(dir):
    print ('\nGetting list of files...')
    listoffiles = getListOfFiles(dir)
    print ('\nGot list of files')
    for file in listoffiles:
        print ('\nProcessing ' + file)
        creationdate = getcdate(file)
        modifieddate = getmdate(file)
        if daysbetween(creationdate, modifieddate) < 0:
            print ('Fixing creation date from ' + creationdate + ' to ' + modifieddate)
            setcdate(file, modifieddate)
    print ('Finished!')


def bulkfixcreationdates(dir):
    t0 = time.time()
    print ('\nGetting list of files in ' + dir + '...')
    listoffiles = getListOfFiles(dir)
    print ('\nGot list of files')
    # n_threads = len(listoffiles)
    with concurrent.futures.ThreadPoolExecutor(48) as executor:
        _ = [executor.submit(fixcreationdate, file) for file in listoffiles]
    t1 = time.time()
    totaltime = t1 -t0
    totaltime = round(totaltime)
    print ('\nFinished in ' + str(totaltime) + ' seconds')

# bulkfixcreationdates (r'G:\Nextcloud\Pictures\James')


print ('All done!')