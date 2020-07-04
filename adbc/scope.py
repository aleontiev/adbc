from fnmatch import fnmatch
from .utils import cached_property, merge
from .exceptions import NotIncluded


def specificity(item):
    index, (key, value) = item
    wildcards = key.count("*")
    inverse = 1 if key.startswith("~") else 0
    others = len(key) - wildcards - inverse
    return (0 if wildcards > 0 else 1, wildcards, others, index)


class WithScope(object):
    def get_scope_translation(self, scope=None, from_=None, to=None):
        if not scope:
            return {}

        scope = scope.get(self.child_key, {})
        translation = {}
        for key, child in scope.items():
            if isinstance(child, dict):
                translate = child.get(from_, None)
                key = child.get(to, key)
                if translate and translate != key:
                    translation[translate] = key
        return translation

    def get_child_include(self, scope=None):
        # TODO: add proper merging of self.config and config
        # so that e.g. a command cannot cross the schema boundary
        # set at DB level

        # as it is, this would allow it
        scope = self.scope if scope is None else scope
        if scope is True or scope is None:
            return True

        return scope.get(self.child_key, True)

    def _get_sorted_child_scopes(self, scope=None):
        scopes = self.get_child_include(scope)
        if scopes is True:
            return {}

        scopes = list(enumerate(scopes.items()))
        scopes.sort(key=specificity)
        return [c[1] for c in scopes]

    @cached_property
    def _sorted_child_scopes(self):
        return self._get_sorted_child_scopes()

    def get_sorted_child_scopes(self, scope=None):
        if scope is None:
            return self._sorted_child_scopes
        else:
            return self._get_sorted_child_scopes(scope=scope)

    def get_child_scope(self, name, scope=None):
        include = self.get_child_include(scope=scope)

        if include is True:
            # empty scope (all included)
            return True

        result = {}

        # merge all matching scope entries
        # go in order from least specific to most specific
        # this means exact-match scopes will take highest precedence
        for key, child in self.get_sorted_child_scopes(scope=scope):
            tag_name = child.get(self.tag, None) if isinstance(child, dict) else None
            # optionally replace key with tagged value from inside child scope
            if tag_name:
                key = tag_name

            inverse = False
            if key.startswith("~"):
                inverse = True
                key = key[1:]
            match = fnmatch(name, key)
            if (match and not inverse) or (not match and inverse):
                # we have a match, merge in the scope
                if child is False:
                    child = {"enabled": False}
                elif child is True:
                    child = {"enabled": True}
                merge(result, child)

        if not result:
            return True

        if not result or (isinstance(result, dict) and not result.get("enabled", True)):
            raise NotIncluded()

        return result
