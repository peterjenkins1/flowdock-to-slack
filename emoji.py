"""
Downloads all the Flowdock emoji's
I found the list of emojis using the Chome dev tools, there isn't much point
in reverse engineering the Flowdock API to get the list.

Once you've got all the emoji's in a directory upload to slack using:
  https://github.com/smartlyio/slack-emojinator/tree/fix_fetch_api_tokens
"""
import wget
import os
import json

def get_flowdock_emojis():
    return load_json_file('test/flowdock-emojis.json')

def load_json_file(path):
    with open(path) as f:
        return json.load(f)

fd_emojis = get_flowdock_emojis()
for emoji in fd_emojis:
    url = emoji['image_url']
    out = 'output/%s.png' % emoji['id']
    if not os.path.isfile(out):
        wget.download(url, out=out)


