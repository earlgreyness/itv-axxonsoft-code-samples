# -*- coding: utf-8 -*-

from pikuli import FindFailed

from . import TIMEZONE
from .gui_element_wrapper import GUIElementWrapper as Wrapper


class Calendar(Wrapper):
    """
    Calendar widget wrapper.
    """
    def __init__(self, searcher, searcher_slider, **kwargs):
        super(Calendar, self).__init__(searcher, **kwargs)
        self.searcher_slider = searcher_slider

    def close(self):
        if not self.present():
            return
        uia = self.uia.find_by_control("ITV.Framework.UI.GraphicControls.Button.SimpleButton")
        uia.region.click()
        assert not self.present()

    def invoke(self):
        if self.present():
            return
        self.searcher_slider.search().region.click()
        assert self.present()

    def enter_datetime(self, datetime):
        d = datetime.to(TIMEZONE)

        name_1 = "ITV.Framework.UI.GraphicControls.Calendar.TimePicker"
        name_2 = "ITV.Framework.UI.GraphicControls.Calendar.Picker"
        name_3 = "ITV.Framework.UI.GraphicControls.Label.Label"
        name_4 = "ITV.Framework.UI.GraphicControls.UniversalControls.TextBox"

        base_picker = self.uia.find_by_control(name_1)

        crit = dict(exact_level=1, LocalizedControlType=name_2)

        pickers = sorted(base_picker.find_all(**crit), key=lambda u: u.region.x)
        assert len(pickers) == 3

        try:
            meridiem = base_picker.find_by_control(name_3, timeout=0.1)
        except FindFailed:
            meridiem = None

        # On some occasions AM/PM label is not present.
        # It means that the time is expected to be
        # in 24 hours format.
        tokens = 'Hms' if meridiem is None else 'hms'

        for p, f in zip(pickers, tokens):
            try:
                uia = p.find_by_control(name_3, timeout=0.1)
            except FindFailed:
                uia = p.find_by_control(name_4)
            uia.region.type(d.format(f), press_enter=True)

        if meridiem is not None and meridiem.Name != d.format('A'):
            # d.format('A') -> 'AM' or 'PM'
            meridiem.region.click()

