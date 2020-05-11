import json
import os
import requests
from time import gmtime, strftime
import shutil
from hashlib import blake2b
import re
from slack import WebClient
from slack.errors import SlackApiError

flowdock_token = os.environ.get('FLOWDOCK_TOKEN')
slack_token = os.environ.get('SLACK_API_TOKEN')
slack_team = os.environ.get('SLACK_TEAM')
flowdock_org = os.environ.get('FLOWDOCK_ORG')
flowdock_url = 'https://api.flowdock.com'
output_path = 'output'
output_dir_prefix = output_path + '/slack-export-'

flowdock_messages_file = 'input/exports/flowdock-replacement/messages.json'

def get_flowdock_url(rest_url):
    flowdock_headers = {'Authorization': 'Basic %s' % flowdock_token}
    r = requests.get(flowdock_url + rest_url,
                     headers=flowdock_headers)
    r.raise_for_status()
    return r.json()

def get_flowdock_users():
    # Temp file with users cached to avoid lots of requests
    users_file = 'test/users.json'
    return load_json_file(users_file)
    #return get_flowdock_url('/organizations/%s/users' % flowdock_org)

def get_slack_users():
    # Temp file with users cached
    users_file = 'test/slack-users.json'
    return load_json_file(users_file)

    """
    client = WebClient(token=slack_token)

    try:
        response = client.users_list()
        return response['members']
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error: {e.response['error']}")
    """

def load_json_file(path):
    with open(path) as f:
        return json.load(f)

def build_fd_to_slack_uid_map(flowdock_users, slack_users):
    slack_users = load_json_file(slack_users_file)

    fd_to_slack_uid_map = {} # dict where key is a flowdock uid and value is a slack uid
    for fd_user in flowdock_users:
        for slack_user in slack_users:
            if slack_user['profile'].get('email') == fd_user['email']:
                fd_to_slack_uid_map[str(fd_user['id'])] = slack_user['id']
    return fd_to_slack_uid_map

def build_fd_users_index(flowdock_users, slack_users):
    fd_users_index = {}
    for user in flowdock_users:
        fd_users_index[user['id']] = user
    return fd_users_index

def transform_fd_messages_to_slack(flowdock_messages, fd_to_slack_uid_map, fd_users_index):
    thread_mapping = {} # maps Flowdock thread_id's to Slack format
    slack_messages = [] # what we return

    for fm in flowdock_messages:
        sm = {} # a single slack message

        # Lookup metadata of user that sent this message
        fd_uid = int(fm['user'])
        fd_user = fd_users_index.get(fd_uid)

        # Map all the fields
        sm['type'] = fm['event']
        sm['text'] = fm['content']
        sm['user'] = fd_to_slack_uid_map.get(str(fd_uid))

        # Slack messages have some undocumented hash
        # 3c0332f2-77d5-404d-a70f-e24f08a39b97
        m = blake2b(str(fm).encode(), digest_size=21).hexdigest()
        sm['client_msg_id'] = '{0}-{1}-{2}-{3}-{4}'.format(m[:8], m[9:13], m[14:18], m[19:23], m[24:36])

        # Slack messages have a timestamp followed by . and 6 digits
        sm_ts = '%s.000000' % fm['sent'] # Unclear what the '.000000' is for
        sm['ts'] = sm_ts

        # thread_ts is the same as ts for unthreaded messages, but for threaded
        # messages it takes the thread_ts of the first message (parent)
        # https://api.slack.com/messaging/retrieving#finding_threads
        if fm['thread_id'] in thread_mapping:
            # This is from a thread so find the Slack thread_ts for it
            sm['thread_ts'] = thread_mapping[fm['thread_id']]
        else:
            # This is a single message
            sm['thread_ts'] = sm_ts
            # Add this to the map in case there are replies later
            thread_mapping[fm['thread_id']] = sm_ts

        sm['team'] = slack_team
        sm['user_team'] = slack_team
        sm['source_team'] = slack_team

        if fd_user:
            sm['user_profile'] = {
                'display_name': fd_user['nick'],
                'first_name': '',
                'real_name': re.split(' - ', fd_user['name'])[0],
                'team': slack_team,
                'is_restricted': False,
                'is_ultra_restricted': False
            }
        sm['blocks'] = [] # for formatting
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
 
def write_output(slack_messages):
    """
    Writes all the messages into the Slack format and creates a zip file for
    import into Slack.
    """
    output_dir = output_dir_prefix + strftime('%Y-%m-%d-%H-%M-%S', gmtime())
    os.mkdir(output_dir)

    # copy the users.json from our test data
    # we can get the current list from the Slack API if needed
    shutil.copy(
        'test/Smartly.io Slack export Mar 30 2020 - Mar 31 2020/users.json',
        output_dir + '/users.json'
    )

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
    shutil.make_archive(
        base_name=output_path + '/latest',
        format='zip',
        root_dir=output_dir
    )

def main():
    slack_users = get_slack_users()
    flowdock_users = get_flowdock_users()
    fd_to_slack_uid_map = build_fd_to_slack_uid_map(flowdock_users, slack_users)
    fd_users_index = build_fd_users_index(flowdock_users, slack_users)
    flowdock_messages = load_json_file(flowdock_messages_file)
    slack_messages = transform_fd_messages_to_slack(flowdock_messages, fd_to_slack_uid_map, fd_users_index)
    write_output(slack_messages)

if __name__ == '__main__':
    main()

"""
TODO:
 - Read in the flowdock exports from emails
 - Download and extract the zip files
   - unzip just messages.json to input/exports/zip-file-name
 - Never set the user as None/null
 - 
"""
