import json
import os
import requests
from time import gmtime, strftime

flowdock_token = os.environ.get('FLOWDOCK_TOKEN')
flowdock_url = 'https://api.flowdock.com'
flowdock_org = 'smartly'
output_path = 'output'
output_dir_prefix = output_path + '/slack-export-'

flowdock_messages_file = 'test/flowdock-replacement-2020-03-31/messages.json'
slack_users_file = 'test/Smartly.io Slack export Mar 30 2020 - Mar 31 2020/users.json'

def get_flowdock_url(rest_url):
    flowdock_headers = {'Authorization': 'Basic %s' % flowdock_token}
    r = requests.get(flowdock_url + rest_url,
                     headers=flowdock_headers)
    r.raise_for_status()
    return r.json()

def get_flowdock_users():
    return get_flowdock_url('/organizations/%s/users' % flowdock_org)

def load_json_file(path):
    with open(path) as f:
        return json.load(f)

def build_fd_to_slack_uid_map():
    #flowdock_users = get_flowdock_users()
    # Temp file with users cached to avoid lots of requests
    users_file = 'test/users.json'
    flowdock_users = load_json_file(users_file)

    slack_users = load_json_file(slack_users_file)

    fd_to_slack_uid_map = {} # dict where key is a flowdock uid and value is a slack uid
    # Should probably sort both lists of users by email address
    for fd_user in flowdock_users:
        for slack_user in slack_users:
            if slack_user['profile'].get('email') == fd_user['email']:
                fd_to_slack_uid_map[str(fd_user['id'])] = slack_user['id']
                #slack_users.remove(slack_user)
                #flowdock_users.remove(fd_user)
    return fd_to_slack_uid_map

def transform_fd_messages_to_slack():
    flowdock_messages = load_json_file(flowdock_messages_file)
    slack_messages = []

    for fm in flowdock_messages:
        sm = {}
        sm['type'] = fm['event']
        sm['text'] = fm['content']
        sm['user'] = fd_to_slack_uid_map.get(fm['user'])
        slack_messages.append(sm)
    return slack_messages

def generate_channels_list():
    return [
        {
            "id": "DEADBEEF",
            "name": "from-flowdock",
            "created": 0,
            "creator": "U010F2VJ92M", # Peter Jenkins
            "is_archived": False,
            "is_general": False,
            "members": [
                "U010F2VJ92M" # Peter Jenkins
            ],
            "topic": {
                "value": "",
                "creator": "",
                "last_set": 0
            },
            "purpose": {
                "value": "",
                "creator": "",
                "last_set": 0
            }
        }
    ]

def write_json_file(contents, path, filename):
    with open(path + '/' + filename, 'w') as f:
        json.dump(contents, f, indent=4)
 
def write_output():
    output_dir = output_dir_prefix + strftime('%Y-%m-%d-%H-%M-%S', gmtime())
    os.mkdir(output_dir)

    write_json_file(generate_channels_list(), output_dir, 'channels.json')

    # write users.json? If we use existing users this might not be needed

    # make a directory per channel
    channels = ['from-flowdock']
    for channel in channels:
        channel_dir = '%s/%s' % (output_dir, channel)
        os.mkdir(channel_dir)
        # write the messages into the channel directory
        write_json_file(slack_messages, channel_dir, 'messages.json')

    # zip everything up

fd_to_slack_uid_map = build_fd_to_slack_uid_map()
slack_messages = transform_fd_messages_to_slack()

write_output()
