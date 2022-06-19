import json


def dictionary_list_to_set(self, list):
    new_set = {json.dumps(dictionary, sort_keys=True) for dictionary in list}
    return new_set
