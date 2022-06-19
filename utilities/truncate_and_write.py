from array import array


import string

def truncate_and_write(filename: str, contents: str):
    file = open(filename, "w")
    file.truncate()
    
    if type(contents) != string:
        contents = str(contents)
    file.write(contents)
    file.close()
    