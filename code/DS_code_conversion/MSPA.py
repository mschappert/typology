import os
import subprocess

# wsl terminal
# cd ~
#wsl
#GWB_MSPA

#powershell terminal
# cd D:\typology\code\DS_code_conversion
# python MSPA.py

# N O T E:
# if first time using type "yes" to accept the GWB license agreement
# make sure that the output folder does not contain any log files from previous runs, it will fail to run

# Folders - use absolute paths
input_dir = r"D:\typology\data\DS_TEST\mosaic_P_recoded"
output_dir = r"D:\typology\data\DS_TEST\MSPA"
os.makedirs(output_dir, exist_ok=True)

# MSPA parameters
# connectivity (8), edge- default (1), transition- off (0), intext = on (1), write to disk with swap (1), statistics- off (0)
params = """8 
1
0
1
1
1
0"""

with open(os.path.join(input_dir, "mspa-parameters.txt"), "w") as f:
    f.write(params)

# convert windows paths to WSL path
input_wsl = subprocess.check_output(["wsl", "wslpath", "-u", input_dir.replace("\\", "/")], text=True).strip()
output_wsl = subprocess.check_output(["wsl", "wslpath", "-u", output_dir.replace("\\", "/")], text=True).strip()

# GWB_MSPA in WSL
subprocess.run([
    "wsl", "GWB_MSPA",
    f"-i={input_wsl}",
    f"-o={output_wsl}"
], check=True)