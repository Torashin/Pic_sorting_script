
import PySimpleGUI as sg
import Pic_sorting_script as pss
import os
import threading

working_directory = os.getcwd()

def create_window():
    sg.theme('DarkBlue')
    layou t= [
        [sg.Text('Select source directory:')],
        [sg.InputText(pss.defaultsourcedir, size=(50 ,1), key = '-sourcedir-'),
         sg.FolderBrowse('Browse', initial_folder=working_directory)],
        [sg.Text('Select destination directory:')],
        [sg.InputText(pss.defaultdestdir, size=(50 ,1), key = '-destdir-'),
         sg.FolderBrowse('Browse', initial_folder=working_directory)],
        [sg.Button('Go!', key = '-go-')],
        [sg.ProgressBar(max_value=1, orientation='h', size=(20, 20), key='-progressbar-')]
    ]

    return sg.Window('Pic Sorting Script' ,layout)

window = create_window()
progress_bar = window['-progressbar-']

while True:
    event, values = window.read(timeout=10)

    if event == sg.WIN_CLOSED:
        break

    if event == '-go-':
        sourcedir = values['-sourcedir-']
        destdir = values['-destdir-']
        print('Source = ' + sourcedir)
        print('Dest = ' + destdir)
        asncfunc = threading.Thread(target=pss.bulkprocess, args=(sourcedir, destdir))
        asncfunc.start()

    if pss.workerrunning == True and pss.filestoprocess != 0 and pss.filesprocessed != 0:
        print ('progress = ' + str(pss.filesprocessed / pss.filestoprocess))
        progress_bar.update_bar(pss.filesprocessed / pss.filestoprocess)


window.close()