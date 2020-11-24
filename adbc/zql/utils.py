def literal(x):
    """Make literal"""
    if isinstance(x, str):
        if x and x[0] == x[-1] and x[0] in '"\'`':
            # already literal quoted
            return x
        # literal quote
        return f'"{x}"'
    if isinstance(x, list):
        # apply to children
        return [literal(x_) for x_ in x]

    if isinstance(x, dict):
        # apply to children
        return {k: literal(v) for k, v in x.items()}

    return x
