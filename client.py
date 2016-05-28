import sys
import os

words = ['verafake']

for word in words:
    bash_command = 'sh client.sh  -c config.json -cmd SEARCH -q ' + word
    os.system(bash_command)
