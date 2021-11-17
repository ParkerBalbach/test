
"""
This has flags for emitting signals to the GUI for live updates on different long-running functions. 
There's also a cancellation, so if the GUI user hits the cancel button, it will modify a global variable and exit the funtion mid-run. 
"""
import os

__author__ = "Jordan Hiatt"

cancel_flag = False
success_flag = False
output_str = ""
# global signal? Should be simple enough to set this
global_signal = None
global_lat_bar = None
global_coords = None
global_addr = None
global_filename = ""
global_csv_folder_path = os.getcwd()+'\\Spreadsheets'

if not os.path.isdir(global_csv_folder_path):
    os.makedirs(os.getcwd()+'\\Spreadsheets')

def emitWorkerProgress(worker, s):
    # I think this will just grab the output_str right above it. Just lets us use variables outside of functions
    global output_str
    output_str += s
    worker.progress.emit(output_str)

# if this is being modified in worker thread methods then it won't change till the very end right? 
# def changeGlobalLat():
#     global global_lat_bar
#     print(global_lat_bar)
#     global_lat_bar.setText = 'this has been modified'