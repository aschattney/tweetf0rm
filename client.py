import sys
import os

#word = '"youtube or youtu.be"'
word = 'youtube,youtu.be'

bash_command = 'sh client.sh  -c config.json -cmd STREAM_TWEETS -q ' + word
#bash_command = 'sh client.sh  -c config.json -cmd SEARCH -q ' + word
os.system(bash_command)
