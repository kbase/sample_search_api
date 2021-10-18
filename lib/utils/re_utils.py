# relation engine utilities

"""
Relation engine API client functions.
"""
import json
import re
import requests

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
# # MAX_ADB_INTEGER = 2**53 - 1

_ADB_KEY_DISALLOWED_CHARS_REGEX = re.compile(r"[^a-zA-Z0-9_\-:\.@\(\)\+,=;\$!\*'%]")


def clean_key(key):
    """
    Swaps disallowed characters for an ArangoDB '_key' with an underscore.
    See https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html
    """
    return _ADB_KEY_DISALLOWED_CHARS_REGEX.sub('_', key)


def get_doc(coll, key, re_api_url, token):
    """Fetch a doc in a collection by key."""
    resp = requests.post(
        re_api_url + '/api/v1/query_results',
        data=json.dumps({
            'query': "for v in @@coll filter v._key == @key limit 1 return v",
            '@coll': coll,
            'key': clean_key(key)
        }),
        headers={'Authorization': token}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def execute_query(query, re_api_url, token, params=None):
    """
    Execute an arbitrary query in the database.
    NOTE: be sure to guard against AQL injection when using this function.
    NOTE: token must be Relation Engine Admin token.
    """
    if not params:
        params = {}
    params['query'] = query
    resp = requests.post(
        re_api_url + '/api/v1/query_results',
        data=json.dumps(params),
        headers={'Authorization': token},
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()
