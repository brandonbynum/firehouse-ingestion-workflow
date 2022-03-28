import json

def dictionary_set_to_list(self, set):
    new_list = [json.loads(dictionary) for dictionary in set]
    return new_list