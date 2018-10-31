###########################################################################
#
# OpenOPC for Python Library Module
#
# Copyright (c) 2007-2012 Barry Barnreiter (barry_b@users.sourceforge.net)
# Copyright (c) 2014 Anton D. Kachalov (mouse@yandex.ru)
# Copyright (c) 2017 José A. Maita (jose.a.maita@gmail.com)
# Copyright (c) 2017 Cédric Hernalsteens (cedric.hernalsteens@gmail.com)
#
###########################################################################


class GroupEvents:
    def __init__(self):
        self.client = current_client

    def OnDataChange(self, TransactionID, NumItems, ClientHandles, ItemValues, Qualities, TimeStamps):
        self.client.callback_queue.put((TransactionID, ClientHandles, ItemValues, Qualities, TimeStamps))
