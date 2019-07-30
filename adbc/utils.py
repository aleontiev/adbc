def get_inex_args(value):
    """Get command args that in/exclude based on a value"""
    includes = []
    excludes = []
    for part in value:
        if part.startswith('~'):
            excludes.append(part[1:])
        else:
            includes.append(part)
    return includes, excludes


def get_inex_query(
    table,
    column,
    includes,
    excludes
):
    """Get query args that in/exclude based on a particular column"""
    if not includes and not excludes:
        return ('', [])

    args = []
    query = []
    count = 1

    if includes:
        for include in includes:
            if '*' in include:
                operator = '~~'
                include = include.replace('*', '%')
            else:
                operator = '='
            args.append(include)
            query.append(
                '({}."{}" {} ${})'.format(
                    table,
                    column,
                    operator,
                    count
                )
            )
            count += 1

    if excludes:
        for exclude in excludes:
            if '*' in exclude:
                operator = '!~~'
                exclude = exclude.replace('*', '%')
            else:
                operator = '!='
            args.append(exclude)
            query.append(
                '({}."{}" {} ${})'.format(
                    table,
                    column,
                    operator,
                    count
                )
            )
            count += 1

    if includes and not excludes:
        union = 'OR'
    else:
        union = 'AND'
    return ' {} '.format(union).join(query), args
