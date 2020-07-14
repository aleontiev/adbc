from adbc.template import resolve_template
from yaml import safe_load as load
from adbc.vault import vault
import copy
import os


def get_initial_context(vault=True, env=True):
    """Return context of available services, such as Vault"""
    context = {}
    if vault:
        context['vault'] = VaultConfig()
    if env:
        context['env'] = dict(os.environ)
    return context


def get_config(filename=None, data=None, context=None):
    if not data:
        if not filename:
            filename = os.environ.get('ADBC_CONFIG_PATH') or 'adbc.yml'
        data = read_config_file(filename)

    initial_context = get_initial_context()
    if not context:
        context = initial_context
    else:
        context.update(initial_context)

    return hydrate_config(
        data,
        context=context
    )


def read_config_file(filename):
    """
    Arguments:
        filename: string
    Return:
        config: dict representing raw config
    """
    with open(filename, "r") as file:
        data = load(file.read())
        if "adbc" not in data:
            raise Exception(f'Invalid config file "{filename}", missing "adbc" block')
        return data


def hydrate_config(config, context=None):
    """Hydrates configuration

    Looks for {{ template.tags }} and executes using context

    Arguments:
        config: string or dict representing configuration data to hydrate
        context: dict of context to pass in

    Return:
        dict of hydrated config
    """
    if config is None or isinstance(config, (bool, float, int)):
        return config
    if isinstance(config, str):
        return resolve_template(config, context)
    if isinstance(config, list):
        return [hydrate_config(c, context) for c in config]

    assert isinstance(config, dict)

    result = {}
    for key, value in config.items():
        keys = []
        alias = None
        # build key(s)
        original = key
        key = resolve_template(key, context)
        if isinstance(key, list):
            # multi-value key
            alias = getattr(key, "__alias__", original)
            for record in key:
                ctx = copy.copy(context)
                ctx[alias] = record
                keys.append((ctx, record))
        else:
            keys = [(context, key)]

        # build value(s)
        for ctx, k in keys:
            result[k] = hydrate_config(value, ctx)

    return result


class VaultConfig(object):
    __FIELDS__ = (
        "args",
        "alias",
        "context",
        "context_key",
        "context_mode",
        "alias_mode",
    )

    def __init__(
        self,
        args=None,
        alias=None,
        context=None,
        context_key=None,
        context_mode=False,
        alias_mode=False,
    ):
        # e.g. ['kv', 'get', 'secret', 'environments']
        self.__args__ = args or []
        self.__context__ = context
        self.__context_key__ = context_key
        self.__context_mode__ = context_mode
        self.__alias_mode__ = alias_mode
        self.__alias__ = alias

    def __getattr__(self, key):
        result = self.__extend__(key)
        return result

    def __produce__(self):
        if self.__context_mode__:
            # still in context mode
            return self.__end_context_mode__().__produce__()
        # TODO: vault integration here
        return vault(self.__args__)

    def __clone__(self, **kwargs):
        for field in self.__FIELDS__:
            if field not in kwargs:
                uf = f"__{field}__"
                kwargs[field] = getattr(self, uf)

        return VaultConfig(**kwargs)

    def __end_context_mode__(self):
        # extract captured context key from context
        context_key = "".join(self.__context_key__)
        args = self.__args__
        if context_key:
            # use it to get a new key from the context
            key = self.__context__[context_key]
            args = copy.copy(args)
            # add the key to the running argument list
            args.append(key)
        else:
            raise Exception("end context mode called without any context key")
        return self.__clone__(args=args, context_mode=False, context_key=None)

    def __extend__(self, key):
        if key.startswith("_") and key.endswith("_"):
            # special key
            if key == "_":
                if self.__context_mode__:
                    return self.__end_context_mode__()
                else:
                    return self.__clone__(context_mode=True)
            elif key == "_as_":
                return self.__clone__(alias_mode=True)
            elif key == "_data_":
                # produce data
                return self.__produce__()
            else:
                raise Exception(f'unexpected path: "{key}"')
        else:
            # normal key
            if self.__alias_mode__:
                # alias and produce data
                self.__alias__ = key
                return self.__produce__()

            args = None
            if self.__context_mode__:
                # build context key
                args = self.__context_key__ or []
            else:
                args = copy.copy(self.__args__)

            args.append(key)
            return self.__clone__(args=args)


class WithAlias(object):
    def __init__(self, *args, **kwargs):
        self.__alias__ = kwargs.get("alias", None)


class AliasDict(WithAlias, dict):
    pass


class AliasList(WithAlias, list):
    pass
