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

import os
import sys
import Pyro4.core

__version__ = '1.3.0'

current_client = None

# Win32 only modules not needed for 'open' protocol mode
if os.name == 'nt':
    try:
        import win32com.client
        import win32com.server.util
        import win32event
        import pythoncom
        import pywintypes
        import openopc.SystemHealth

        # Win32 variant types
        pywintypes.datetime = pywintypes.TimeType
        vt = dict([(pythoncom.__dict__[vtype], vtype) for vtype in pythoncom.__dict__.keys() if vtype[:2] == "VT"])

        # Allow gencache to create the cached wrapper objects
        win32com.client.gencache.is_readonly = False

        # Under p2exe the call in gencache to __init__() does not happen
        # so we use Rebuild() to force the creation of the gen_py folder
        win32com.client.gencache.Rebuild(verbose=0)

    # So we can work on Windows in "open" protocol mode without the need for the win32com modules
    except ImportError:
        win32com_found = False
    else:
        win32com_found = True
else:
    win32com_found = False
    
print("Found win32com ?? %d" % win32com_found)

# OPC Constants
SOURCE_CACHE = 1
SOURCE_DEVICE = 2
OPC_STATUS = (0, 'Running', 'Failed', 'NoConfig', 'Suspended', 'Test')
BROWSER_TYPE = (0, 'Hierarchical', 'Flat')
ACCESS_RIGHTS = (0, 'Read', 'Write', 'Read/Write')
OPC_QUALITY = ('Bad', 'Uncertain', 'Unknown', 'Good')
OPC_CLASS = "Matrikon.OPC.Automation;" \
            "Graybox.OPC.DAWrapper;" \
            "HSCOPC.Automation;" \
            "RSI.OPCAutomation;" \
            "OPC.Automation"
OPC_SERVER = "Hci.TPNServer;" \
             "HwHsc.OPCServer;" \
             "opc.deltav.1;" \
             "AIM.OPC.1;" \
             "Yokogawa.ExaopcDAEXQ.1;" \
             "OSI.DA.1;" \
             "OPC.PHDServerDA.1;" \
             "Aspen.Infoplus21_DA.1;" \
             "National Instruments.OPCLabVIEW;" \
             "RSLinx OPC Server;" \
             "KEPware.KEPServerEx.V4;" \
             "Matrikon.OPC.Simulation;" \
             "Prosys.OPC.Simulation;" \
             "CCOPC.XMLWrapper.1;" \
             "OPC.SimaticHMI.CoRtHmiRTm.1"
OPC_CLIENT = 'OpenOPC'


def quality_str(quality_bits):
    """Convert OPC quality bits to a descriptive string."""
    quality = (quality_bits >> 6) & 3
    return OPC_QUALITY[quality]


def type_check(tags):
    """Perform a type check on a list of tags."""
    if type(tags) in (list, tuple):
        single = False
    elif tags is None:
        tags = []
        single = False
    else:
        tags = [tags]
        single = True

    if len([t for t in tags if type(t) not in (str, bytes)]) == 0:
        valid = True
    else:
        valid = False

    return tags, single, valid


def wild2regex(s):
    """Convert a Unix wildcard glob into a regular expression"""
    return s.replace('.', '\.').replace('*', '.*').replace('?', '.').replace('!', '^')


def tags2trace(tags):
    """Convert a list tags into a formatted string suitable for the trace callback log"""
    arg_str = ''
    for i, t in enumerate(tags[1:]):
        if i > 0:
            arg_str += ','
        arg_str += '%s' % t
    return arg_str


def exceptional(func, alt_return=None, alt_exceptions=(Exception,), final=None, catch=None):
    """Turns exceptions into an alternative return value."""
    def _exceptional(*args, **kwargs):
        try:
            try:
                return func(*args, **kwargs)
            except alt_exceptions:
                return alt_return
            except:
                if catch: return catch(sys.exc_info(), lambda: func(*args, **kwargs))
                raise
        finally:
            if final: final()

    return _exceptional


def get_sessions(host='localhost', port=7766):
    """Return sessions in OpenOPC Gateway Service as GUID:host hash."""
    import Pyro4.core
    server_obj = Pyro4.Proxy("PYRO:opc@{0}:{1}".format(host, port))
    return server_obj.get_clients()


def open_client(host='localhost', port=7766, **kwargs):
    """Connect to the specified OpenOPC Gateway Service."""
    server_obj = Pyro4.Proxy("PYRO:opc@{0}:{1}".format(host, port))
    return server_obj.create_client()
