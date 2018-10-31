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
import select
import os
import time
#import openopc
from openopc.test import *
from openopc.Client import Client

try:
    import Pyro4.core
    # import Pyro4.protocol
except ImportError:
    print('Pyro4 module required (https://pypi.python.org/pypi/Pyro4)')
    exit()

Pyro4.config.SERVERTYPE = 'thread'
# Pyro4.config.SERIALIZER='marshal'

opc_class = openopc.OPC_CLASS
opc_gate_host = os.environ.get('OPC_GATE_HOST', '172.16.23.112')
opc_gate_port = int(os.environ.get('OPC_GATE_PORT', 7766))

def getvar(env_var):
    """Read system environment variable from registry"""
    try:
        key = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE,
                              'SYSTEM\\CurrentControlSet\\Control\Session Manager\Environment', 0, _winreg.KEY_READ)
        value, valuetype = _winreg.QueryValueEx(key, env_var)
        return value
    except:
        return None


# Get env vars directly from the Registry since a reboot is normally required
# for the Local System account to inherit these.

# if getvar('OPC_CLASS'):  opc_class = getvar('OPC_CLASS')
# if getvar('OPC_GATE_HOST'):  opc_gate_host = getvar('OPC_GATE_HOST')
# if getvar('OPC_GATE_PORT'):  opc_gate_port = int(getvar('OPC_GATE_PORT'))

@Pyro4.expose  # needed for version 4.55
class OPCGateway(object):
    def __init__(self):
        self._remote_hosts = {}
        self._init_times = {}
        self._tx_times = {}

    def get_clients(self):
        """Return list of server instances as a list of (GUID,host,time) tuples"""

        # reg = Pyro4.core.DaemonObject(self._pyroDaemon).registered()[2:]
        reg1 = Pyro4.core.DaemonObject(self._pyroDaemon).registered()  # needed for version 4.55
        reg2 = [si for si in reg1 if si.find('obj_') == 0]
        reg = ["PYRO:{0}@{1}:{2}".format(obj, opc_gate_host, opc_gate_port) for obj in reg2]
        hosts = self._remote_hosts
        init_times = self._init_times
        tx_times = self._tx_times
        hlist = [(hosts[k] if k in hosts else '', init_times[k], tx_times[k]) for k in reg]
        return hlist

    def create_client(self):
        """Create a new OpenOPC instance in the Pyro server"""
        print("Pompidou 1")
        opc_obj = Client(opc_class)
        print("Pompidou 2")

        uri = self._pyroDaemon.register(opc_obj)
        print("Pompidou 3")

        uuid = uri.asString()
        opc_obj._open_serv = self
        opc_obj._open_self = opc_obj
        opc_obj._open_host = opc_gate_host
        opc_obj._open_port = opc_gate_port
        opc_obj._open_guid = uuid
        
        print("Pompidou 4")

        remote_ip = uuid  # self.getLocalStorage().caller.addr[0]
        #        try:
        #            remote_name = socket.gethostbyaddr(remote_ip)[0]
        #            self._remote_hosts[uuid] = '%s (%s)' % (remote_ip, remote_name)
        #        except socket.herror:
        #            self._remote_hosts[uuid] = '%s' % (remote_ip)
        self._remote_hosts[uuid] = '%s' % (remote_ip)
        self._init_times[uuid] = time.time()
        self._tx_times[uuid] = time.time()
        print("Pompidou 5")
        return Pyro4.Proxy(uri)

    def release_client(self, obj):
        """Release an OpenOPC instance in the Pyro server"""

        self._pyroDaemon.unregister(obj)
        del self._remote_hosts[obj.GUID()]
        del self._init_times[obj.GUID()]
        del self._tx_times[obj.GUID()]
        del obj



if __name__ == '__main__':

    daemon = Pyro4.core.Daemon(host=opc_gate_host, port=60000)
    daemon.register(OPCGateway(), 'opc')
    socks = set(daemon.sockets)
    print("Starting OpenOPCService in the foreground on port {}.".format(60000))
    while True:
        ins, outs, exs = select.select(socks, [], [], 1)
        if ins:
            daemon.events(ins)

    daemon.shutdown()

