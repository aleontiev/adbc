import subprocess
import json


def vault(args):
    num_args = len(args)
    assert(num_args > 2)
    command, subcommand = args[0:2]
    if command != 'kv':
        raise NotImplementedError(command)
    if subcommand not in {'get', 'list'}:
        raise NotImplementedError(subcommand)

    name = args[-1]
    call = args[0:2]
    call.append('-format=json')
    call.append('/'.join(args[2:]))
    result = subprocess.check_output(
        f'vault {" ".join(call)} 2> /dev/null',
        shell=True
    )
    result = json.loads(result)
    if subcommand == 'get':
        result = result['data']['data']
        if len(result) == 1 and name in result:
            result = result[name]
    else:
        result = [
            r.replace('/', '') for r in result
        ]
    return result
