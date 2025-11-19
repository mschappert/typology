import os
import subprocess

# wsl terminal
#wsl
#GWB_MSPA

#powershell terminal
# cd D:\typology\code\DS_code_conversion
# python MSPA.py

# if first time using type "yes" to accept the GWB license agreement

# Folders
input_dir = "D:/typology/data/DS_TEST/mosaic_P_recoded"
output_dir = "D:/typology/data/DS_TEST/MSPA"
os.makedirs(output_dir, exist_ok=True)

# MSPA parameters
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