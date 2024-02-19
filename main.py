
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

import PySimpleGUI as sg
import funcs as pssfuncs
import os
import threading

class PicSortingScriptGUI:
    def __init__(self):
        self.working_directory = os.getcwd()
        self.statusbar_color = ('#1c1c1c', '#ffffff')
        self.status = 'Not ready'
        self.total_files = 0
        self.files_processed = 0
        self.status_lock = threading.Lock()
        self.window = self.create_window()

    @property
    def percent_processed(self):
        if self.total_files > 0 and self.files_processed > 0:
            return (self.files_processed / self.total_files) * 100
        else:
            return 0

    def create_window(self):
        sg.theme('DarkBlue')

        main_layout = [
            [sg.Text('Select source directory:', pad=(5, (5, 0)))],
            [sg.InputText(pssfuncs.defaultsourcedir, size=(50, 1), key='-sourcedir-'),
             sg.FolderBrowse('Browse', initial_folder=self.working_directory)],
            [sg.Text('Select destination directory:', pad=(5, (10, 0)))],
            [sg.InputText(pssfuncs.defaultdestdir, size=(50, 1), key='-destdir-'),
             sg.FolderBrowse('Browse', initial_folder=self.working_directory)],
            [sg.Frame('Options', [
                [sg.Text('File Move Type:')],
                [sg.Radio('Copy', 'file_move_type', key='-copy-', default=True),
                 sg.Radio('Move', 'file_move_type', key='-move-')],
                [sg.Text('File Naming:')],
                [sg.Radio('Use Original', 'file_naming', key='-use_original-', default=True),
                 sg.Radio('Generate from Metadata', 'file_naming', key='-generate-')],
                [sg.Text('File date priority:')],
                [sg.Radio('Prioritise metadata date', 'file_date_priority', key='-metadate-', default=True),
                 sg.Radio('Prioritise file date', 'file_date_priority', key='-filedate-')],
                [sg.Text('Require folder date match?')],
                [sg.Radio('No', 'need_folderdate', key='-folderdate_false-', default=True),
                 sg.Radio('Yes', 'need_folderdate', key='-folderdate_true-')],
                [sg.Text('Update file creation date?')],
                [sg.Radio('No', 'update_file_date', key='-update_file_date_false-', default=True),
                 sg.Radio('Yes', 'update_file_date', key='-update_file_date_true-')],
                [sg.Text('Update metadata date?')],
                [sg.Radio('No', 'update_meta_date', key='-update_meta_date_false-', default=True),
                 sg.Radio('Yes', 'update_meta_date', key='-update_meta_date_true-')],
            ], pad=(5, (15, 5)))],
            [sg.Push(), sg.Button('Go!', key='-go-', size=(10, 1), pad=((5, 5), (20, 15)))],
        ]

        status_bar_layout = [
            [sg.StatusBar(f'Status: {self.status}', key='-statusbar-', size=(50, 1), pad=(5, 5),
                          relief=sg.RELIEF_FLAT, text_color=self.statusbar_color[1],
                          background_color=self.statusbar_color[0], font=('Any', 10, 'bold'))]
        ]

        layout = main_layout + status_bar_layout
        return sg.Window('Pic Sorting Script', layout, finalize=True)

    def update_status(self, status_msg=None):
        with self.status_lock:
            if status_msg:
                self.status = status_msg
            elif self.total_files > 0:
                self.status = f'Processed {self.files_processed} of {self.total_files} files ({self.percent_processed:.1f}% complete)'
            else:
                self.status = 'Ready'
            if self.window:
                self.window['-statusbar-'].update(f'Status: {self.status}', text_color=self.statusbar_color[1],
                                                  background_color=self.statusbar_color[0])

    def run(self):
        self.update_status()
        while True:
            event, values = self.window.read(timeout=100)  # Increase the timeout for smoother status bar updates
            if event == sg.WIN_CLOSED:
                break
            elif event == '-go-':
                # Retrieve values from GUI elements
                sourcedir = values['-sourcedir-']
                destdir = values['-destdir-']
                move_files = True if values['-move-'] else False
                rename_files = True if values['-generate-'] else False
                filedate_beats_metadadate = False if values['-filedate-'] else False
                need_folderdate_match = True if values['-folderdate_true-'] else False
                update_file_date = True if values['-update_file_date_true-'] else False
                update_meta_date = True if values['-update_meta_date_true-'] else False

                # Update status bar
                self.total_files = 0  # Set the total number of files (replace with actual count)
                self.files_processed = 0
                self.update_status()

                # Perform processing in a separate thread
                async_func = threading.Thread(
                    target=pssfuncs.bulkprocess,
                    args=(sourcedir, destdir, self, rename_files, move_files, need_folderdate_match,
                          filedate_beats_metadadate,update_file_date, update_meta_date)
                )
                async_func.start()

                # Optionally wait for the thread to finish before updating the status bar back to "Ready"
                # async_func.join()

                # Note: You may want to update self.files_processed within the bulkprocess function to indicate the progress.

                # Update status bar back to "Ready"
                self.update_status()

        self.window.close()

# Instantiate and run the GUI
if __name__ == '__main__':
    gui = PicSortingScriptGUI()
    gui.run()
