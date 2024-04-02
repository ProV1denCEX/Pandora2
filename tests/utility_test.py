# -*- coding:utf-8 -*-
import datetime

from Pandora.helper.date import Dates, DateFmt


def test_date_convert():
    assert "2021-11-22" == Dates.convert("20211122")
    assert "20211122" == Dates.convert("2021/11/22", fmt=DateFmt.YMD)

    tday = datetime.date.today()
    tday = tday.replace(2021, 11, 22)
    assert "2021-11-22" == Dates.convert(tday)
