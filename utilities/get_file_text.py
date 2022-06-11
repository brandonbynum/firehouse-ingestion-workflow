def get_file_text(filename):
    file = open(filename, "r")
    contents = file.read()
    file.close()
    return contents