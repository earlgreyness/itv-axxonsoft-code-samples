# -*- coding: utf-8 -*-

import re

from .gui_element_exception import GUIElementPublicException as PublicError


def find_nested(searcher, *criteria_list):
    element = searcher.search()
    return element.find_nested(*criteria_list)


def find_nested_by_control(searcher, *steps):
    crits = [{'LocalizedControlType': s, 'exact_level': 1} for s in steps]
    return find_nested(searcher, *crits)


class UIAElementSearcher(object):
    def __init__(self, target_func, *args, **kwargs):
        self._target_func = target_func
        self._args = args
        self._kwargs = kwargs
        self._found_uia_elem = None

    @classmethod
    def init_from_found(cls, found_uia_elem):
        searcher = cls(None)
        searcher._found_uia_elem = found_uia_elem
        return searcher

    def search(self):
        if self._target_func is not None:
            return self._target_func(*self._args, **self._kwargs)
        elif self._found_uia_elem is not None:
            return self._found_uia_elem
        raise PublicError('Searcher badly initialized. Inner state not consistent.',
                          is_internal_error=False)


class Singleton(type):
    """
    For more details see
    http://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def camera_full_name_regexp_by_id(display_id):
    return re.compile('^{}..*$'.format(str(display_id)))
