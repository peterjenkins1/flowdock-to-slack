import json
import os
import requests

flowdock_token = os.environ.get('FLOWDOCK_TOKEN')
flowdock_headers = {'Authorization': 'Basic %s' % flowdock_token}
flowdock_url = 'https://api.flowdock.com'
flowdock_org = 'smartly'

def get_flowdock_url(rest_url):
    r = requests.get(flowdock_url + rest_url,
                     headers=flowdock_headers)
    r.raise_for_status()
    return r.json()

def get_flowdock_users():
    return get_flowdock_url('/organizations/%s/users' % flowdock_org)

def load_json_file(path):
    with open(path) as f:
        return json.load(f)

def map_fd_uid_to_slack_uid(fd_uid):
    """
    This is going to be quite slow if we do a lot of lookups. We could pre-process all
    the ID pairs into a simple dict. That would also help find issues.
    """
    email = ""
    for user in flowdock_users:
        if str(user['id']) == fd_uid:
            email = user['email']
            break
    else:
        return None
    for user in slack_users:
        if user['profile'].get('email') == email:
            return user['id']
        else:
            continue
    return None
# flowdock_users = get_flowdock_users()
# Temp file with users cached to avoid lots of requests
users_file = 'test/users.json'
flowdock_users = load_json_file(users_file)

flowdock_messages_file = 'test/flowdock-replacement-2020-03-31/messages.json'
flowdock_messages = load_json_file(flowdock_messages_file)

slack_users_file = 'test/Smartly.io Slack export Mar 30 2020 - Mar 31 2020/users.json'
slack_users = load_json_file(slack_users_file)

slack_messages = []

for fm in flowdock_messages:
    sm = {}
    sm['type'] = fm['event']
    sm['text'] = fm['content']
    sm['user'] = map_fd_uid_to_slack_uid(fm['user'])
    slack_messages.append(sm)
