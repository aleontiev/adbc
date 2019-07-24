def get_include_exclude(value):
    parts = value.split(',')
    includes = []
    excludes = []
    for part in parts:
        if part.startswith('!'):
            excludes.append(part[1:])
        else:
            includes.append(part)
    return includes, excludes
