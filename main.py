
import PySimpleGUI as sg
import funcs as pssfuncs
import os
import threading

working_directory = os.getcwd()

def create_window():
    sg.theme('DarkBlue')
    layout= [
        [sg.Text('Select source directory:')],
        [sg.InputText(pssfuncs.defaultsourcedir, size=(50 , 1), key ='-sourcedir-'),
         sg.FolderBrowse('Browse', initial_folder=working_directory)],
        [sg.Text('Select destination directory:')],
        [sg.InputText(pssfuncs.defaultdestdir, size=(50 , 1), key ='-destdir-'),
         sg.FolderBrowse('Browse', initial_folder=working_directory)],
        [sg.Button('Go!', key = '-go-')],
    ]
    return sg.Window('Pic Sorting Script' ,layout)

window = create_window()

while True:
    event, values = window.read(timeout=10)
    if event == sg.WIN_CLOSED:
        break
    if event == '-go-':
        sourcedir = values['-sourcedir-']
        destdir = values['-destdir-']
        print('Source = ' + sourcedir)
        print('Dest = ' + destdir)
        asncfunc = threading.Thread(target=pssfuncs.bulkprocess, args=(sourcedir, destdir))
        asncfunc.start()

window.close()