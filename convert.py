import json
import os
import requests
from time import gmtime, strftime
import shutil
from hashlib import blake2b
import re
from slack import WebClient
from slack.errors import SlackApiError
from datetime import datetime
from dateutil.relativedelta import relativedelta

flowdock_token = os.environ.get('FLOWDOCK_TOKEN')
slack_token = os.environ.get('SLACK_API_TOKEN')
slack_team = os.environ.get('SLACK_TEAM')
flowdock_org = os.environ.get('FLOWDOCK_ORG')
flowdock_url = 'https://api.flowdock.com'
import_dir = 'input/exports' # Contains a directory per flow
output_path = 'output'
cache_dir = 'cache'
output_dir_prefix = output_path + '/slack-export-'
export_channel_prefix = 'history-'
import_bot_id = 'U01360Y5U9W' # Flowdock migration bot

flowdock_messages_file = 'input/exports/flowdock-replacement/messages.json'

def get_flowdock_url(rest_url):
    flowdock_headers = {'Authorization': 'Basic %s' % flowdock_token}
    r = requests.get(flowdock_url + rest_url,
                     headers=flowdock_headers)
    r.raise_for_status()
    return r.json()

def get_from_cache(cache_file):
    cache_file_rel_path = '%s/%s' % (cache_dir, cache_file)

    if os.path.exists(cache_file_rel_path):
        one_day_ago = datetime.now() - relativedelta(days=1)
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_file_rel_path))
        if file_time > one_day_ago:
            # Cache hit
            return load_json_file(cache_file_rel_path)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

    return None

def get_flowdock_users():
    cache_file = 'flowdock-users.json'

    flowdock_users = get_from_cache(cache_file)
    if flowdock_users:
        return flowdock_users

    flowdock_users = get_flowdock_url('/organizations/%s/users' % flowdock_org)
    write_json_file(flowdock_users, cache_dir, cache_file)
    return flowdock_users

def get_slack_users():
    cache_file = 'slack-users.json'
    
    slack_users = get_from_cache(cache_file)
    if slack_users:
        return slack_users
    
    # Cache doesn't exist or is stale, so fetch the list from Slack

    client = WebClient(token=slack_token)

    try:
        response = client.users_list()
        slack_users = response['members']
        # Write list to cache
        write_json_file(slack_users, cache_dir, cache_file)
        return slack_users
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error: {e.response['error']}")

def load_json_file(path):
    with open(path) as f:
        return json.load(f)

def build_fd_uid_to_slack_user_map(flowdock_users, slack_users):
    fd_to_slack_uid_map = {} # dict where key is a flowdock uid and value is a slack user
    for fd_user in flowdock_users:
        fd_uid = str(fd_user['id']) # keys as strings because they are strings in the messages
        for slack_user in slack_users:
            if (slack_user['profile'].get('email') == fd_user['email'] or
                slack_user.get('real_name') == fd_user['name'].split(' - ')[0]):
                fd_to_slack_uid_map[fd_uid] = slack_user
                break
        if not fd_to_slack_uid_map.get(fd_uid):
            print('No match for %s - %s - %s' % (fd_user['email'], fd_user['nick'], fd_user['name']))
            
    return fd_to_slack_uid_map

def build_fd_users_index(flowdock_users, slack_users):
    fd_users_index = {}
    for user in flowdock_users:
        fd_users_index[str(user['id'])] = user
    return fd_users_index

def generate_flowdock_thread_backlink_message(fm, flow, parent):
    fd_thread_url = 'https://www.flowdock.com/app/{org}/{flow}/threads/{thread_id}'.format(
        org = flowdock_org, flow = flow, thread_id = fm['thread_id']
    )
    m = blake2b(str(fm).encode(), digest_size=21).hexdigest()
    msg_id = '{0}-{1}-{2}-{3}-{4}'.format(m[:8], m[9:13], m[14:18], m[19:23], m[24:36])
    return {
        'type': 'message',
        'text': fd_thread_url,
        'ts': '%d.%06d' % divmod(fm['sent'], 1e3),
        'user': import_bot_id,
        'thread_ts': parent['ts'],
        'parent_user_id': parent['user'],
        'team': slack_team,
        'user_team': slack_team,
        'source_team': slack_team,
        'client_msg_id': msg_id,
        'user_profile': {
            'image_72': 'https://avatars.slack-edge.com/2020-06-02/1171563963329_423169057c2045a4a24f_72.jpg',
            'avatar_hash': '423169057c20',
            'display_name': 'Flowdock',
            'first_name': 'flowdock_migration',
            'real_name': 'flowdock_migration',
            'team': slack_team,
            'name': 'flowdock',
            'is_restricted': False,
            'is_ultra_restricted': False
        }
    }

def format_slack_mention(handle):
    # '@Foo' becomes '<@foo>'
    return '<%s>' % handle.group(0).lower()

def transform_fd_message_to_slack(fm, slack_user):
    """
    Map the simpler fields
    """

    no_attachements_explanation = 'This message was imported from Flowdock and the attachement was not.\n'

    # sm = slack message
    sm = {}

    # turn '@Foo' into '<@foo>' for Slack to see the mentions
    regex = r"(@\w+)"
    slack_text = re.sub(regex, format_slack_mention, str(fm['content']), 0, re.MULTILINE)

    if fm['event'] == 'file':
        sm['type'] = 'message'
        sm['text'] = no_attachements_explanation + slack_text
    elif fm['event'] == 'message':
        sm['type'] = 'message'
        sm['text'] = slack_text
    elif fm['event'] == 'comment':
        sm['type'] = 'message'
        sm['text'] = slack_text
    else:
        print('Skipping message of unknown type %s' % fm['event'])
        return

    sm['user'] = slack_user['id']

    # Slack messages have an undocumented hash like this:
    # 3c0332f2-77d5-404d-a70f-e24f08a39b97
    # make up some random hash that looks the same!
    m = blake2b(str(fm).encode(), digest_size=21).hexdigest()
    sm['client_msg_id'] = '{0}-{1}-{2}-{3}-{4}'.format(m[:8], m[9:13], m[14:18], m[19:23], m[24:36])

    sm['team'] = slack_team
    sm['user_team'] = slack_team
    sm['source_team'] = slack_team

    sm['user_profile'] = {
        'image_72': slack_user['profile']['image_72'],
        'avatar_hash': slack_user['profile']['avatar_hash'],
        'display_name': slack_user['profile']['display_name'],
        'real_name': slack_user['profile']['real_name'],
        'team': slack_team,
        'name': slack_user['name'],
        'is_restricted': False,
        'is_ultra_restricted': False
    }
    #sm['blocks'] = [] # for formatted messages

    return sm

def transform_fd_messages_to_slack(flowdock_messages, flow, fd_uid_to_slack_user_map, fd_users_index):
    thread_mapping = {} # maps Flowdock thread_id's to Slack parent messages
    slack_messages = [] # what we return, a list of messages ready for import

    for fm in flowdock_messages:

        # Lookup metadata of user that sent this message
        fd_uid = fm['user']

        flowdock_user = fd_users_index.get(fd_uid) # Can be None
        slack_user = fd_uid_to_slack_user_map.get(fd_uid) # Can be None

        if not slack_user:
            slack_user = {
                'id': import_bot_id,
                'name': flowdock_user['email'].split('@')[0] if flowdock_user else 'unknown',
                'profile': {
                    'display_name': flowdock_user['nick'] if flowdock_user else 'unknown',
                    'image_72': '',
                    'avatar_hash': '',
                    'real_name': re.split(' - ', flowdock_user['name'])[0] if flowdock_user else 'unknown'
                }
            }

        # sm is a single slack message to add to the list
        sm = transform_fd_message_to_slack(fm, slack_user)
        if not sm:
            # Allow transform_fd_message_to_slack to skip messages
            continue

        # Slack messages have a timestamp followed by . and 6 digits
        sm_ts = '%d.%06d' % divmod(fm['sent'], 1e3)
        sm['ts'] = sm_ts

        """
        Threadding: is where this gets ugly
       
        thread_ts is the same as ts for unthreaded messages, but for threaded
        messages it takes the thread_ts of the first message (parent)
        https://api.slack.com/messaging/retrieving#finding_threads

        Slack also needs the parent of each thread to contain a list of
        all the replies along with other metadata. Since we are processing
        messages seqentially we need to maintain a list of references to these 
        parent messages so we can update them.

        thread_mapping is a dict where Flowdock thread_id is the key. The value is 
        a reference to a previous message.

        """

        if fm.get('thread_id') in thread_mapping:
            # This message is from a thread

            # We need a reference to the parent
            parent = thread_mapping[fm['thread_id']]

            # Add required keys to the current message
            sm['thread_ts'] = parent['ts']
            sm['parent_user_id'] = parent['user']

            # Several updates to the parent message to reflect this new reply

            # Parent messages need a list of replies
            if not parent.get('reply_count'):
                # this is the first reply

                # add a message with the old Flowdock thread in it
                thread_backlink = generate_flowdock_thread_backlink_message(
                    fm, flow, parent
                )
                slack_messages.append(thread_backlink)

                # Initialise the thead metadata with this first reply
                parent['replies'] = [{
                    'user': import_bot_id,
                    'ts': parent['ts']
                }]
                parent['reply_users'] = [import_bot_id]
                parent['reply_users_count'] = 1

            replies = parent['replies']
            replies.append({
                'user': slack_user['id'],
                'ts': sm_ts
            })
            parent['reply_count'] = len(replies)

            # Parent messages also have a list and count of users
            if not slack_user['id'] in parent['reply_users']:
                parent['reply_users'].append(slack_user['id'])
                parent['reply_users_count'] = len(parent['reply_users'])

            # some misc fields
            parent['latest_reply'] = sm_ts
            parent['last_read'] = sm_ts # mark all the imports as read
            parent['subscribed'] = False

        else:
            # This is a single message
            sm['thread_ts'] = sm_ts

            if 'thread_id' in fm:
                # Add this message to the map in case there are replies later
                thread_mapping[fm['thread_id']] = sm

        slack_messages.append(sm)
    return slack_messages

def generate_channels_list(flows):
    channels = []
    count = 0
    for flow in flows:
        channels.append({
            "id": str(count),
            "name": export_channel_prefix + flow,
            "created": 0,
            "creator": "U010F2VJ92M", # Peter Jenkins
            "is_archived": False,
            "is_general": False,
            "members": [],
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
        })
        count += 1
    return channels

def write_json_file(contents, path, filename):
    with open(path + '/' + filename, 'w') as f:
        json.dump(contents, f, indent=4)
 
def write_output(flows, slack_users, fd_uid_to_slack_user_map, fd_users_index):
    """
    Writes all the messages into the Slack format and creates a zip file for
    import into Slack.
    """
    output_dir = output_dir_prefix + strftime('%Y-%m-%d-%H-%M-%S', gmtime())
    os.mkdir(output_dir)

    write_json_file(slack_users, output_dir, 'users.json')
    write_json_file(generate_channels_list(flows), output_dir, 'channels.json')

    # Transform the messages from each flow to a Slack channel
    for flow in flows:
        flowdock_messages = load_json_file('input/exports/%s/messages.json' % flow)
        slack_messages = transform_fd_messages_to_slack(flowdock_messages, flow, fd_uid_to_slack_user_map, fd_users_index)
        
        # make a directory per channel
        channel_dir = '%s/%s%s' % (output_dir, export_channel_prefix, flow)
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
    fd_uid_to_slack_user_map = build_fd_uid_to_slack_user_map(flowdock_users, slack_users)
    fd_users_index = build_fd_users_index(flowdock_users, slack_users)
    flows = os.listdir(import_dir)
    write_output(flows, slack_users, fd_uid_to_slack_user_map, fd_users_index)

if __name__ == '__main__':
    main()

"""
TODO:
 - Read in the flowdock exports from emails
 - Download and extract the zip files
   - unzip just messages.json to input/exports/zip-file-name
 - Handle attachements?!
 - Output multiple channels?
 - Add the avatar etc from Slack profile to each message
 - Reaction emojis
 - Insert Flowdock thread URL as first reply
"""
