#!/usr/bin/env python
# -*- coding: utf-8 -*-

import arrow

from . import LOCALE, TIMEZONE
from .gui_element_wrapper import GUIElementWrapper as Wrapper

PIXELS_TOLERANCE = 4


class Slider(Wrapper):
    """
    Класс, описывающий "бегунок" в панели архива.
    """

    TEMPLATE = 'DD-MMM-YY HH:mm:ss'
    CONTROL_TYPE = 'ITV.Framework.UI.GraphicControls.Timeline.Archive.Slider'
    CONTROL_TYPE_LABEL = 'ITV.Framework.UI.GraphicControls.Label.Label'

    @classmethod
    def _get_arrow(cls, stamp):
        return arrow.get(stamp, cls.TEMPLATE, locale=LOCALE, tzinfo=TIMEZONE)

    def get_datetime_indicated(self):
        criteria = {
            'LocalizedControlType': self.CONTROL_TYPE_LABEL,
            'exact_level': 1,
        }
        elements = self._uia_root.find_all(**criteria)
        # Unpacking implicitly requires that there must be exactly two elements.
        n1, n2 = (element.Name for element in elements)
        stamp_variant_1 = '{0} {1}'.format(n1, n2)
        stamp_variant_2 = '{1} {0}'.format(n1, n2)
        # Exactly one of the two variants must pass the parsing process.
        try:
            d = self._get_arrow(stamp_variant_1)
        except arrow.parser.ParserError:
            d = self._get_arrow(stamp_variant_2)
        return d

    def is_centered_vertically(self, location):
        y_own = self.region.getCenter().y
        y_loc = location.y
        return abs(y_own - y_loc) < PIXELS_TOLERANCE

    def drag_vertically(self, v_coord):
        center = self.region.getCenter()
        center.dragndrop(center.x, v_coord)
