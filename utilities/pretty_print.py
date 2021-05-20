import json

def pretty_print(dict, should_sort_keys):
    print(json.dumps(dict, indent=4, sort_keys=should_sort_keys))