def print_query(query, params, sep='\n-----\n'):
    if not params:
        return query
    else:
        args = '\n'.join([f'${i+1}: {a}' for i, a in enumerate(params)])
        return f'{query}{sep}{args}'
