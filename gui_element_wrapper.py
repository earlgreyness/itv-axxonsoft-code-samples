# -*- coding: utf-8 -*-

import json
import logging
import functools

import pikuli
import pikuli.uia_element
from . import ScrollingDirection
from .gui_element_exception import GUIElementPublicException
from .helpers import UIAElementSearcher as Searcher

logger = logging.getLogger(__name__)


class GUIElementWrapper(object):
    """
    Базовый класс для оберток над элементами GUI.
    Условимся:
        -- Если метод возвращает объект-обертку над дочерним элементом интерфейса, то каждый раз
           происходит поиск через UIA API этого самого дочернего контрола. Никаких кешей.
        -- Это у нас wrapper. Он не хранит в себе никаких состояний контролов -- все запрашивается
           черех UIA API.
    """

    Controls = None
    _COTROL_GETTERS = None

    def __init__(self, uia_elem_searcher, validator=None, parent=None):
        """
        uia_elem_searcher  --  Экземпляр класс UIAElementSearcher. Он знает, как найти UIA-элемент,
                               корневой для текущего wrapper'а.
        parent             --  Cсылка на родительский wrapper-объект по иерархии UIA.
        """
        self.searcher = uia_elem_searcher
        self.validator = validator
        self.parent = parent

    @classmethod
    def from_uia_element(cls, uia_element, **kwargs):
        searcher = Searcher.init_from_found(uia_element)
        return cls(searcher, **kwargs)

    @property
    def _uia_root(self):
        return self.searcher.search()

    def check_internal(self):
        # To be overridden.
        pass

    def check(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            if self.validator is not None:
                self.validator.validate()
            self.check_internal()
            return f(*args, **kwargs)
        return decorated

    @property
    def uia(self):
        """
        Convenience property.
        Rationale: code clarity and succinctness.
        """
        return self._uia_root

    @property
    def region(self):
        return self._uia_root.reg(get_client_rect_by_hwnd=False)

    def present(self, timeout=0.5):
        pikuli.uia_element.DYNAMIC_FIND_TIMEOUT = timeout
        try:
            # This assignment can only be successful
            # if corresponding UIA element
            # is present in the tree. Otherwise
            # an exception is raised.
            # If an element is not in the tree
            # then it is not present on the screen.
            _ = self.searcher.search()
        except pikuli.FindFailed:
            return False
        finally:
            pikuli.uia_element.DYNAMIC_FIND_TIMEOUT = None
        return True

    def click(self):
        # What this method does is considered basic and obvious.
        # Simply click on the center of the element's rectangle.
        # This is commonly used hence there is a solid reason for putting
        # the method inside GUI wrapper base class.
        # An alternative solution might include mixin class `ClickableMixin`
        # that defines this method only.
        self.region.click()

    def scroll(self, direction, iterations=1):
        if direction is ScrollingDirection.WHEEL_UP:
            d = 1
        else:
            logger.warning('May have chosen wrong direction for WHEEL_DOWN scrolling')
            d = 0  # or 2 ?
        self.region.getCenter().scroll(direction=d, count=iterations, click=False)

    def is_enabled(self, ctrl_id):
        """
        Этот метод принимает на вход идентификатор элемента области GUI, поверх которой работает
        wrapper, и возвращает его UIA-свойство IsEnabled.

        :param Enum ctrl_id: Идентификатор какого-то элемента области GUI
        """
        if self.Controls is None:
            raise GUIElementPublicException('Can\'t get control ctrl_id={}: \'Controls\' enum'
                ' isn\'t defined in {}'.format(ctrl_id, type(self)))
        if ctrl_id not in self.Controls:
            raise GUIElementPublicException('Usupported ctrl_id = {}. You should use {} = {}'.format(
                ctrl_id, self.Controls, [e.name for e in self.Controls]))

        ctrl = self._get_control_by_id(ctrl_id)
        try:
            return ctrl.IsEnabled
        except Exception as ex:
            raise GUIElementPublicException('Can\'t get IsEnabled property for ctrl_id={}: {}'.format(
                ctrl_id, ex))

    def _get_control_by_id(self, ctrl_id):
        """
        Возвращает :class:`uia_element` для контрола, на который указывает ctrl_id

        :param ctrl_id type: элемент enum'a :class:`ArchiveSettings`.Controls
        """
        if self._COTROL_GETTERS is None:
            raise GUIElementPublicException('Can\'t get control ctrl_id={}: \'_COTROL_GETTERS\' dict'
                ' isn\'t defined in {}'.format(ctrl_id, type(self)))
        control = getattr(self, self._COTROL_GETTERS[ctrl_id], None)  # Именно контролл, т.к. там property-методы
        if control is None:
            GUIElementPublicException('ID {} isn\'t listed in _COTROL_GETTERS'.format(ctrl_id),
                is_internal_error=True)
        return control

    @staticmethod
    def _get_help_text(uia_elem):
        """
        Возвращает в dict структуру JSON, содержащуюся в поле HelpText у uia_elem. Если HelpText
        нет, то будет возвращен пустой словарь.
        """
        try:
            help_text = uia_elem.HelpText
        except Exception as ex:
            raise GUIElementPublicException(
                'Can not get HelpText from {}'.format(uia_elem),
                prev_exception=ex)
        try:
            return json.loads(help_text or {})
        except ValueError as ex:
            raise GUIElementPublicException(
                'Can not parse HelpText of {} as JSON'.format(uia_elem),
                prev_exception=ex)
