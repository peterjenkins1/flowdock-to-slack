import json
import yaml
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
import time
import textwrap

def load_configuration():
    with open(config_file) as f:
        return yaml.safe_load(f)

config_file = 'config.yml'
config = load_configuration()
slack_team = config['slack_team']
flowdock_org = config['flowdock_org']
flowdock_url = 'https://api.flowdock.com'
import_dir = 'input/exports' # Contains a directory per flow
output_path = 'output'
cache_dir = 'cache'
output_dir_prefix = output_path + '/slack-export-'
export_channel_prefix = 'history-'
import_bot_slack_id = config['import_bot_slack_id']

flowdock_messages_file = 'input/exports/flowdock-replacement/messages.json'

def get_flowdock_url(rest_url, params={}):
    flowdock_headers = {'Authorization': 'Basic %s' % config['flowdock_token']}
    r = requests.get(flowdock_url + rest_url,
                     headers=flowdock_headers,
                     params=params)
    r.raise_for_status()
    return r.json()

def get_from_cache(cache_file):
    cache_file_rel_path = '%s/%s' % (cache_dir, cache_file)

    if os.path.exists(cache_file_rel_path):
        one_month_ago = datetime.now() - relativedelta(month=1)
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_file_rel_path))
        if file_time > one_month_ago:
            # Cache hit
            return load_json_file(cache_file_rel_path)
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

    client = WebClient(token=config['slack_api_token'])

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
    fd_to_slack_user_map = {} # dict where key is a flowdock uid and value is a slack user
    for fd_user in flowdock_users:
        fd_uid = str(fd_user['id']) # keys as strings because they are strings in the messages
        for slack_user in slack_users:
            if (slack_user['profile'].get('email') == fd_user['email'] or
                slack_user.get('real_name') == fd_user['name'].split(' - ')[0]):
                fd_to_slack_user_map[fd_uid] = slack_user
                break
        if not fd_to_slack_user_map.get(fd_uid):
            print('No match for %s - %s - %s' % (fd_user['email'], fd_user['nick'], fd_user['name']))
            
    return fd_to_slack_user_map

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
        'user': import_bot_slack_id,
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

def transform_fd_message_to_slack(fm, slack_user, fd_uid_to_slack_user_map):
    """
    Map the simpler fields
    """

    no_attachements_explanation = '''
    This message was imported from Flowdock and the attachement was not.

    The original filename was: '''

    # sm = slack message
    sm = {}

    # turn '@Foo' into '<@foo>' for Slack to see the mentions
    regex = r"(@+\w+)"
    slack_text = re.sub(regex, format_slack_mention, str(fm['content']), 0, re.MULTILINE)

    if fm['event'] == 'file':
        sm['type'] = 'message'
        sm['text'] = no_attachements_explanation + fm['content']['file_name']
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

    sm['reactions'] = []
    for emoji in fm['emojiReactions']:
        users = []
        for fd_uid in fm['emojiReactions'][emoji]:
            slack_user = fd_uid_to_slack_user_map.get(fd_uid)
            if slack_user:
                users.append(slack_user['id'])
            else:
                # Avoid duplicate bot users
                if import_bot_slack_id not in users:
                    users.append(import_bot_slack_id)

        sm['reactions'].append({
            'name': emoji,
            'users': users,
            'count': len(users)
        })

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
                'id': import_bot_slack_id,
                'name': flowdock_user['email'].split('@')[0] if flowdock_user else 'unknown',
                'profile': {
                    'display_name': flowdock_user['nick'] if flowdock_user else 'unknown',
                    'image_72': '',
                    'avatar_hash': '',
                    'real_name': re.split(' - ', flowdock_user['name'])[0] if flowdock_user else 'unknown'
                }
            }

        # sm is a single slack message to add to the list
        sm = transform_fd_message_to_slack(fm, slack_user, fd_uid_to_slack_user_map)
        if not sm:
            # Allow transform_fd_message_to_slack to skip messages
            continue

        # Messages get truncated around 4000 characters. We make it a bit shorter
        # so we can add an explation for the user.
        slack_message_max_length = 3900
        multipart_message_list = []
        if len(sm['text']) > slack_message_max_length:
            # We have a long message, we need to split the text into several parts
            multipart_message_list = textwrap.wrap(sm['text'], width=slack_message_max_length, replace_whitespace=False)

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

        # This message might be part of an existing thread. We try to find the parent
        parent = thread_mapping.get(fm.get('thread_id'))

        if not parent and not multipart_message_list:
            # This is a single message which is not too long
            sm['thread_ts'] = sm_ts

            if 'thread_id' in fm:
                # Add this message to the map in case there are replies later
                thread_mapping[fm['thread_id']] = sm

            slack_messages.append(sm)

        if parent or multipart_message_list:
            # This message is from a thread and/or this message is so long we need
            # to make it a thread to group all the parts

            # If this is long message and first in the thread set the parent to the
            # current message
            if not parent:
                parent = sm
                # Add this message to the map in case there are replies later
                if fm.get('thread_id'):
                    thread_mapping[fm['thread_id']] = sm

            # Add keys to the current message to make it a thread
            sm['thread_ts'] = parent['ts']
            sm['parent_user_id'] = parent['user']

            if multipart_message_list:
                # Copy sm so we can reuse it for the multi-part messages
                sm_copy = sm.copy()
            else:
                sm_copy = None

            # Several updates to the parent message to reflect this new reply

            # Parent messages need a list of replies
            if not parent.get('reply_count'):
                # this is the first reply

                # add a message with the old Flowdock thread in it
                # Some very old messages might not have a thread_id
                if fm.get('thread_id'):
                    thread_backlink = generate_flowdock_thread_backlink_message(
                        fm, flow, parent
                    )
                    slack_messages.append(thread_backlink)

                # Initialise the thead metadata with this first reply
                parent['replies'] = [{
                    'user': import_bot_slack_id,
                    'ts': parent['ts']
                }]
                parent['reply_users'] = [import_bot_slack_id]
                parent['reply_users_count'] = 1

            replies = parent['replies']
            replies.append({
                'user': slack_user['id'],
                'ts': sm_ts
            })
            parent['reply_count'] = len(replies)

            # We want to mark threads as read so we need to track the last/final
            # timestamp. Initialise it to the current one.
            last_ts = sm_ts

            # Append the current message to the list before we handle
            # multi-part messages and update the parent
            if not multipart_message_list:
                slack_messages.append(sm)
                pass
            else:

                # Handle long multi-part messages
                for count, text_part in enumerate(multipart_message_list):
                    # Use a copy of the current message before we added all the thread crap to it
                    part = sm_copy.copy() if sm_copy else sm.copy()
                    # Increment the timestamp to add these messages to the thread after
                    # the first one. Each message is one second? appart
                    part_ts = sm['ts'].split('.')
                    part_ts[0] = int(part_ts[0]) + count + 1
                    part_ts[1] = int(part_ts[1]) + count + 1
                    last_ts = '%d.%06d' % tuple(part_ts)
                    part['ts'] = last_ts
                    if count == 0:
                        part['text'] = text_part
                    else:
                        part['text'] = '*Flowdock imported message continues ...*\n' + text_part
                    # append a message for each part
                    slack_messages.append(part)
                    # update the parent
                    replies = parent['replies']
                    replies.append({
                        'user': slack_user['id'],
                        'ts': last_ts
                    })
                    parent['reply_count'] = len(replies)

            # Parent messages also have a list and count of users
            # We only need to update this once for multi-part messages
            if not slack_user['id'] in parent['reply_users']:
                parent['reply_users'].append(slack_user['id'])
                parent['reply_users_count'] = len(parent['reply_users'])

            # some misc fields
            parent['latest_reply'] = last_ts
            parent['last_read'] = last_ts # mark all the imports as read
            parent['subscribed'] = False

    return slack_messages

def generate_channels_list(flows):
    channels = []
    count = 0
    for flow_name in flows:
        channels.append({
            "id": str(count),
            "name": export_channel_prefix + flow_name,
            "created": 0,
            "creator": "U010F2VJ92M", # Peter Jenkins
            "is_archived": True,
            "is_general": False,
            "members": [],
            "topic": {
                "value": "Former %s Flow from Flowdock" % flow_name,
                "creator": "",
                "last_set": 0
            },
            "purpose": {
                "value": "Former %s Flow from Flowdock. This is now a read-only archive." % flow_name,
                "creator": "",
                "last_set": 0
            }
        })
        count += 1
    return channels

def write_json_file(contents, path, filename):
    with open(path + '/' + filename, 'w') as f:
        json.dump(contents, f, indent=4)

def get_flow_messages(flow_name, flow_param):
    """
    Flowdock messages are paginated so we need to fetch them 100 at a time
    """
    cache_file = 'flow-%s.json' % flow_param

    messages = get_from_cache(cache_file)

    if messages:
        print('Found cached messages for %s AKA %s' % (flow_name, flow_param))
        return messages

    print('Downloading messages from %s AKA %s' % (flow_name, flow_param))
    messages = []
    id = 0 # We start with the oldest possible message

    flowdock_headers = {'Authorization': 'Basic %s' % config['flowdock_token']}
    messages_url = '{url}/flows/{org}/{flow}/messages'.format(
        url=flowdock_url,
        org=flowdock_org,
        flow=flow_param
    )
    session = requests.Session()
    session.headers=flowdock_headers

    # Check if we can read the flow and fix if needed
    flow_url = '{url}/flows/{org}/{flow}'.format(url=flowdock_url, org=flowdock_org, flow=flow_param)
    r = session.get(flow_url)
    flow_metadata = r.json()
    if not flow_metadata['open']:
        r = session.put(flow_url, data={'open': True})
        r.raise_for_status()

    while True:
        print('.', end='', flush=True)
        r = session.get(messages_url, params={
            'event': 'file,message,comment',
            'limit': 100, # we fetch the max possible messages
            'since_id': id,
            'sort': 'asc'
        })
        r.raise_for_status()
        page = r.json()
        if page:
            id = page[-1]['id'] # move the id to the last message
            messages.extend(page)
        else:
            print()
            break

    write_json_file(messages, cache_dir, cache_file)
    return messages

def get_all_flows():
    cache_file = 'all-flows.json'

    all_flows = get_from_cache(cache_file)
    if all_flows:
        return all_flows

    all_flows = get_flowdock_url('/flows/all')
    write_json_file(all_flows, cache_dir, cache_file)
    return all_flows

def migrate_flows_to_slack_format(slack_users, fd_uid_to_slack_user_map, fd_users_index):
    """
    Writes all the messages into the Slack format and creates a zip file for
    import into Slack.
    """
    output_dir = output_dir_prefix + strftime('%Y-%m-%d-%H-%M-%S', gmtime())
    os.mkdir(output_dir)

    write_json_file(slack_users, output_dir, 'users.json')

    # We import the list of flows from our config file AND any that are under
    # input/exports/*/messages.json
    # We do this because the larger exports are huge and the zip files are always unreadable
    # We can't get private flows using the API, so we need to do both :-()
    api_flows = config['api_flows'] 
    exported_flows = config['exported_flows']
    
    # Transform the messages from each flow to a Slack channel
    for flow in exported_flows:
        try:
            flowdock_messages = load_json_file('input/exports/%s/messages.json' % flow)
            transform_and_write_messages(flowdock_messages, flow, flow, fd_uid_to_slack_user_map, fd_users_index, output_dir)
        except:
            print('Could not find downloaded messages for %s' % flow)

    # Flows have a name which may be different from what the backend expects
    # the client normally maps the display name ('name') to the real name 
    # ('parameterized_name')
    all_flows = get_all_flows()

    name_to_param_name_map = {}
    for flow in all_flows:
        name_to_param_name_map[flow['name']] = flow['parameterized_name']

    # Filter the dict of flows_name -> flow_param to include only the ones from config.yml
    flows = { flow_name:flow_param for (flow_name,flow_param) in name_to_param_name_map.items() if flow_name in api_flows }

    for flow_name, flow_param in flows.items():

        flowdock_messages = get_flow_messages(flow_name, flow_param)
        transform_and_write_messages(flowdock_messages, flow_param, flow_name, fd_uid_to_slack_user_map, fd_users_index, output_dir)

    write_json_file(generate_channels_list(flows), output_dir, 'channels.json')

    # zip everything up
    shutil.make_archive(
        base_name=output_path + '/latest',
        format='zip',
        root_dir=output_dir
    )

def transform_and_write_messages(flowdock_messages, flow_param, flow_name, fd_uid_to_slack_user_map, fd_users_index, output_dir):
    slack_messages = transform_fd_messages_to_slack(flowdock_messages, flow_param, fd_uid_to_slack_user_map, fd_users_index)
    # make a directory per channel
    channel_dir = '%s/%s%s' % (output_dir, export_channel_prefix, flow_name)
    os.mkdir(channel_dir)
    # write the messages into the channel directory
    write_json_file(slack_messages, channel_dir, 'messages.json')

def main():
    slack_users = get_slack_users()
    flowdock_users = get_flowdock_users()
    fd_uid_to_slack_user_map = build_fd_uid_to_slack_user_map(flowdock_users, slack_users)
    fd_users_index = build_fd_users_index(flowdock_users, slack_users)
    migrate_flows_to_slack_format(slack_users, fd_uid_to_slack_user_map, fd_users_index)
    
if __name__ == '__main__':
    main()

"""
TODO:
 - Handle attachements?
 - Reaction emojis - did this but Slack can't import it's own exports!
"""
