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
import time
import string
import socket
import re
import Pyro4.core
from multiprocessing import Queue
from .GroupEvents import GroupEvents
from .Common import *
from .Simu import *
import sys

# For simulation only
simu = 0
if 'OPC_SIMU' in os.environ:
    simu  = os.environ['OPC_SIMU']
    if simu == 1:
        from .Simu import *
        
print("Simu %s" % simu)


@Pyro4.expose
class Client:
    def __init__(self, opc_class=None, client_name=None):
        """Instantiate OPC Client object."""
        self.callback_queue = Queue()

        pythoncom.CoInitialize()

        if opc_class is None:
            if 'OPC_CLASS' in os.environ:
                opc_class = os.environ['OPC_CLASS']
            else:
                opc_class = OPC_CLASS

        opc_class_list = opc_class.split(';')

        for i, c in enumerate(opc_class_list):
            try:
                self._opc = win32com.client.gencache.EnsureDispatch(c, 0)
                self.opc_class = c
                break
            except pythoncom.com_error as err:
                if i == len(opc_class_list) - 1:
                    print("Error Dispatch %s" % self._get_error_str(err))
                    raise
            except:
                print("Error %s" % sys.exc_info()[1])
                raise

        self._event = win32event.CreateEvent(None, 0, 0, None)

        self.opc_server = None
        self.opc_host = None
        self.client_name = client_name
        self._groups = {}
        self._group_tags = {}
        self._group_valid_tags = {}
        self._group_server_handles = {}
        self._group_handles_tag = {}
        self._group_hooks = {}
        self._open_serv = None
        self._open_self = None
        self._open_guid = None
        self._prev_serv_time = None
        self._tx_id = 0
        self.trace = None
        self.cpu = None


    def connect(self, opc_host='localhost', opc_server=None):
        """Connect to the specified OPC server"""

        try:
            pythoncom.CoInitialize()
        except pythoncom.com_error as err:
            print("Error Connect %s" % self._get_error_str(err))
            raise
                
        if opc_server is None:
            # Initial connect using environment vars
            if self.opc_server is None:
                if 'OPC_SERVER' in os.environ:
                    opc_server = os.environ['OPC_SERVER']
                else:
                    opc_server = OPC_SERVER
            # Reconnect using previous server name
            else:
                opc_server = self.opc_server
                opc_host = self.opc_host

        opc_server_list = opc_server.split(';')
        connected = False

        for s in opc_server_list:
            try:
                if self.trace: self.trace('Connect(%s,%s)' % (s, opc_host))
                self._opc.Connect(s, opc_host)
                print("Connected to %s - %s" % (s, opc_host))
            except pythoncom.com_error as err:
                if len(opc_server_list) == 1:
                    print("Error Connect %s" % self._get_error_str(err))
                    raise
            else:
                # Set client name since some OPC servers use it for security
                try:
                    if self.client_name is None:
                        if 'OPC_CLIENT' in os.environ:
                            self._opc.ClientName = os.environ['OPC_CLIENT']
                        else:
                            self._opc.ClientName = OPC_CLIENT
                    else:
                        self._opc.ClientName = self.client_name
                except:
                    pass
                connected = True
                break

        if not connected:
            raise Exception('Connect: Cannot connect to any of the servers in the OPC_SERVER list')

        # With some OPC servers, the next OPC call immediately after Connect()
        # will occationally fail.  Sleeping for 1/100 second seems to fix this.
        time.sleep(0.01)

        self.opc_server = opc_server
        if opc_host == 'localhost':
            opc_host = socket.gethostname()
        self.opc_host = opc_host

        # On reconnect we need to remove the old group names from OpenOPC's internal
        # cache since they are now invalid
        self._groups = {}
        self._group_tags = {}
        self._group_valid_tags = {}
        self._group_server_handles = {}
        self._group_handles_tag = {}
        self._group_hooks = {}


    def GUID(self):
        return self._open_guid


    def close(self, del_object=True):
        """Disconnect from the currently connected OPC server"""
        #print("Trying to close the connection with %s" % self.opc_host)
        opc_host = self.opc_host
        try:
            pythoncom.CoInitialize()
            self.remove(self.groups())

        except pythoncom.com_error as err:
            print('Disconnect: %s' % self._get_error_str(err))
            raise

        except Exception:
            pass

        finally:
            if self.trace: self.trace('Disconnect()')
            self._opc.Disconnect()

            # Remove this object from the open gateway service
            if self._open_serv and del_object:
                self._open_serv.release_client(self._open_self)
                
            print("Disconnected from %s" % (opc_host))
            

    def iread(self, tags=None, group=None, size=None, pause=0, source='hybrid', update=-1, timeout=5000, sync=False,
              include_error=False, rebuild=False):
        """Iterable version of read()"""

        def add_items(tags):
            names = list(tags)

            names.insert(0, 0)
            errors = []

            if self.trace: self.trace('Validate(%s)' % tags2trace(names))

            try:
                errors = opc_items.Validate(len(names) - 1, names)
            except:
                pass

            valid_tags = []
            valid_values = []
            client_handles = []

            if sub_group not in self._group_handles_tag:
                self._group_handles_tag[sub_group] = {}
                n = 0
            elif len(self._group_handles_tag[sub_group]) > 0:
                n = max(self._group_handles_tag[sub_group]) + 1
            else:
                n = 0

            for i, tag in enumerate(tags):
                if errors[i] == 0:
                    valid_tags.append(tag)
                    client_handles.append(n)
                    self._group_handles_tag[sub_group][n] = tag
                    n += 1
                elif include_error:
                    error_msgs[tag] = self._opc.GetErrorString(errors[i])

                if self.trace and errors[i] != 0: self.trace('%s failed validation' % tag)

            client_handles.insert(0, 0)
            valid_tags.insert(0, 0)
            server_handles = []
            errors = []

            if self.trace: self.trace('AddItems(%s)' % tags2trace(valid_tags))

            try:
                server_handles, errors = opc_items.AddItems(len(client_handles) - 1, valid_tags, client_handles)
            except:
                pass

            valid_tags_tmp = []
            server_handles_tmp = []
            valid_tags.pop(0)

            if not sub_group in self._group_server_handles:
                self._group_server_handles[sub_group] = {}

            for i, tag in enumerate(valid_tags):
                if errors[i] == 0:
                    valid_tags_tmp.append(tag)
                    server_handles_tmp.append(server_handles[i])
                    self._group_server_handles[sub_group][tag] = server_handles[i]
                elif include_error:
                    error_msgs[tag] = self._opc.GetErrorString(errors[i])

            valid_tags = valid_tags_tmp
            server_handles = server_handles_tmp

            return valid_tags, server_handles

        def remove_items(tags):
            if self.trace:
                self.trace('RemoveItems(%s)' % tags2trace([''] + tags))
            server_handles = [self._group_server_handles[sub_group][t] for t in tags]
            server_handles.insert(0, 0)
            errors = []

            try:
                errors = opc_items.Remove(len(server_handles) - 1, server_handles)
            except pythoncom.com_error as err:
                print('RemoveItems: %s' % self._get_error_str(err))
                raise

                
        status = []
        
        try:
            pythoncom.CoInitialize()

            if include_error:
                sync = True

            if sync:
                update = -1

            tags, single, valid = type_check(tags)
            if not valid:
                raise TypeError("iread(): 'tags' parameter must be a string or a list of strings")

            # Group exists
            if group in self._groups and not rebuild:
                num_groups = self._groups[group]
                data_source = SOURCE_CACHE

            # Group non-existant
            else:
                if size:
                    # Break-up tags into groups of 'size' tags
                    tag_groups = [tags[i:i + size] for i in range(0, len(tags), size)]
                else:
                    tag_groups = [tags]

                num_groups = len(tag_groups)
                data_source = SOURCE_DEVICE

            results = []

            for gid in range(num_groups):
                if gid > 0 and pause > 0: time.sleep(pause / 1000.0)

                error_msgs = {}
                opc_groups = self._opc.OPCGroups
                opc_groups.DefaultGroupUpdateRate = update

                # Anonymous group
                if group is None:
                    try:
                        if self.trace:
                            self.trace('AddGroup()')
                        opc_group = opc_groups.Add()
                    except pythoncom.com_error as err:
                        print('AddGroup: %s' % self._get_error_str(err))
                        raise
                    sub_group = group
                    new_group = True
                else:
                    sub_group = '%s.%d' % (group, gid)

                    # Existing named group
                    try:
                        if self.trace: self.trace('GetOPCGroup(%s)' % sub_group)
                        opc_group = opc_groups.GetOPCGroup(sub_group)
                        new_group = False

                    # New named group
                    except:
                        try:
                            if self.trace: self.trace('AddGroup(%s)' % sub_group)
                            opc_group = opc_groups.Add(sub_group)
                        except pythoncom.com_error as err:
                            print('AddGroup: %s' % self._get_error_str(err))
                            raise
                        self._groups[str(group)] = len(tag_groups)
                        new_group = True

                opc_items = opc_group.OPCItems

                if new_group:
                    opc_group.IsSubscribed = 1
                    opc_group.IsActive = 1
                    if not sync:
                        if self.trace: self.trace('WithEvents(%s)' % opc_group.Name)
                        self._group_hooks[opc_group.Name] = win32com.client.WithEvents(opc_group, GroupEvents)
                        self._group_hooks[opc_group.Name].set_client(self)

                    tags = tag_groups[gid]

                    valid_tags, server_handles = add_items(tags)

                    self._group_tags[sub_group] = tags
                    self._group_valid_tags[sub_group] = valid_tags

                # Rebuild existing group
                elif rebuild:
                    tags = tag_groups[gid]

                    valid_tags = self._group_valid_tags[sub_group]
                    add_tags = [t for t in tags if t not in valid_tags]
                    del_tags = [t for t in valid_tags if t not in tags]

                    if len(add_tags) > 0:
                        valid_tags, server_handles = add_items(add_tags)
                        valid_tags = self._group_valid_tags[sub_group] + valid_tags

                    if len(del_tags) > 0:
                        remove_items(del_tags)
                        valid_tags = [t for t in valid_tags if t not in del_tags]

                    self._group_tags[sub_group] = tags
                    self._group_valid_tags[sub_group] = valid_tags

                    if source == 'hybrid': data_source = SOURCE_DEVICE

                # Existing group
                else:
                    tags = self._group_tags[sub_group]
                    valid_tags = self._group_valid_tags[sub_group]
                    if sync:
                        server_handles = [item.ServerHandle for item in opc_items]

                tag_value = {}
                tag_quality = {}
                tag_time = {}
                tag_error = {}

                # Sync Read
                if sync:
                    values = []
                    errors = []
                    qualities = []
                    timestamps = []

                    if len(valid_tags) > 0:
                        server_handles.insert(0, 0)

                        if source != 'hybrid':
                            data_source = SOURCE_CACHE if source == 'cache' else SOURCE_DEVICE

                        if self.trace: self.trace('SyncRead(%s)' % data_source)

                        try:
                            values, errors, qualities, timestamps = opc_group.SyncRead(data_source,
                                                                                       len(server_handles) - 1,
                                                                                       server_handles)
                        except pythoncom.com_error as err:
                            print('SyncRead: %s' % self._get_error_str(err))
                            raise
                        
                        for i, tag in enumerate(valid_tags):
                            tag_value[tag] = values[i]
                            tag_quality[tag] = qualities[i]
                            tag_time[tag] = timestamps[i]
                            tag_error[tag] = errors[i]

                # Async Read
                else:
                    if len(valid_tags) > 0:
                        if self._tx_id >= 0xFFFF:
                            self._tx_id = 0
                        self._tx_id += 1

                        if source != 'hybrid':
                            data_source = SOURCE_CACHE if source == 'cache' else SOURCE_DEVICE

                        if self.trace: self.trace('AsyncRefresh(%s)' % data_source)

                        try:
                            opc_group.AsyncRefresh(data_source, self._tx_id)
                        except pythoncom.com_error as err:
                            print('AsyncRefresh: %s' % self._get_error_str(err))
                            raise

                        tx_id = 0
                        start = time.time() * 1000

                        while tx_id != self._tx_id:
                            now = time.time() * 1000
                            if now - start > timeout:
                                raise Exception('Callback: Timeout waiting for data')

                            if self.callback_queue.empty():
                                pythoncom.PumpWaitingMessages()
                            else:
                                tx_id, handles, values, qualities, timestamps = self.callback_queue.get()

                        for i, h in enumerate(handles):
                            tag = self._group_handles_tag[sub_group][h]
                            tag_value[tag] = values[i]
                            tag_quality[tag] = qualities[i]
                            tag_time[tag] = timestamps[i]

                for tag in tags:
                    if tag in tag_value:
                        if (not sync and len(valid_tags) > 0) or (sync and tag_error[tag] == 0):
                            value = tag_value[tag]
                            if type(value) == pywintypes.TimeType:
                                value = str(value)
                            quality = quality_str(tag_quality[tag])
                            timestamp = str(tag_time[tag])
                        else:
                            value = None
                            quality = 'Error'
                            timestamp = None
                        if include_error:
                            error_msgs[tag] = self._opc.GetErrorString(tag_error[tag]).strip('\r\n')
                    else:
                        value = None
                        quality = 'Error'
                        timestamp = None
                        if tag in include_error and not error_msgs:
                            error_msgs[tag] = ''

                    if single:
                        if include_error:
                            status.append((value, quality, timestamp, error_msgs[tag]))
                        else:
                            status.append((value, quality, timestamp))
                    else:
                        if include_error:
                            status.append((tag, value, quality, timestamp, error_msgs[tag]))
                        else:
                            status.append((tag, value, quality, timestamp))

                if group is None:
                    try:
                        if not sync and opc_group.Name in self._group_hooks:
                            if self.trace: self.trace('CloseEvents(%s)' % opc_group.Name)
                            self._group_hooks[opc_group.Name].close()

                        if self.trace: self.trace('RemoveGroup(%s)' % opc_group.Name)
                        opc_groups.Remove(opc_group.Name)

                    except pythoncom.com_error as err:
                        print('RemoveGroup: %s' % self._get_error_str(err))
                        raise

        except pythoncom.com_error as err:
            print('read: %s' % self._get_error_str(err))
            raise
            
        return status


    def read(self, tags=None, group=None, size=None, pause=0, source='hybrid', update=-1, timeout=5000, sync=False,
             include_error=False, rebuild=False):
        """Return list of (value, quality, time) tuples for the specified tag(s)"""

        #print("Reading %s" % tags)
        
        tags_list, single, valid = type_check(tags)
        if not valid:
            raise TypeError("read(): 'tags' parameter must be a string or a list of strings")

        num_health_tags = len([t for t in tags_list if t[:1] == '@'])
        num_opc_tags = len([t for t in tags_list if t[:1] != '@'])

        if num_health_tags > 0:
            if num_opc_tags > 0:
                raise TypeError("read(): system health and OPC tags cannot be included in the same group")
            results = self._read_health(tags)
        else:
            results = self.iread(tags, group, size, pause, source, update, timeout, sync, include_error, rebuild)

        if single:
            return list(results)[0]
        else:
            return list(results)


    def iwrite(self, tag_value_pairs, size=None, pause=0, include_error=False):
        """Iterable version of write()"""
        
        #print("iwrite tag %s with value %d" % (tag_value_pairs[0], tag_value_pairs[1]))
        
        result = []

        try:
            pythoncom.CoInitialize()

            def _valid_pair(p):
                if type(p) in (list, tuple) and len(p) >= 2 and type(p[0]) in (str, bytes):
                    return True
                else:
                    return False

            if type(tag_value_pairs) not in (list, tuple):
                raise TypeError(
                    "write(): 'tag_value_pairs' parameter must be a (tag, value) tuple or a list of (tag,value) tuples")

            if tag_value_pairs is None:
                tag_value_pairs = ['']
                single = False
            elif type(tag_value_pairs[0]) in (str, bytes):
                tag_value_pairs = [tag_value_pairs]
                single = True
            else:
                single = False

            invalid_pairs = [p for p in tag_value_pairs if not _valid_pair(p)]
            if len(invalid_pairs) > 0:
                raise TypeError(
                    "write(): 'tag_value_pairs' parameter must be a (tag, value) tuple or a list of (tag,value) tuples")

            names = [tag[0] for tag in tag_value_pairs]
            tags = [tag[0] for tag in tag_value_pairs]
            values = [tag[1] for tag in tag_value_pairs]
            
            # Break-up tags & values into groups of 'size' tags
            if size:
                name_groups = [names[i:i + size] for i in range(0, len(names), size)]
                tag_groups = [tags[i:i + size] for i in range(0, len(tags), size)]
                value_groups = [values[i:i + size] for i in range(0, len(values), size)]
            else:
                name_groups = [names]
                tag_groups = [tags]
                value_groups = [values]

            num_groups = len(tag_groups)

            status = []

            for gid in range(num_groups):
                if gid > 0 and pause > 0: time.sleep(pause / 1000.0)

                opc_groups = self._opc.OPCGroups
                opc_group = opc_groups.Add()
                opc_items = opc_group.OPCItems

                names = name_groups[gid]
                tags = tag_groups[gid]
                values = value_groups[gid]

                names.insert(0, 0)
                errors = []

                try:
                    errors = opc_items.Validate(len(names) - 1, names)
                except:
                    pass

                n = 1
                valid_tags = []
                valid_values = []
                client_handles = []
                error_msgs = {}
                
                for i, tag in enumerate(tags):
                    if errors[i] == 0:
                        valid_tags.append(tag)
                        valid_values.append(values[i])
                        client_handles.append(n)
                        error_msgs[tag] = ''
                        n += 1
                    elif include_error:
                        error_msgs[tag] = self._opc.GetErrorString(errors[i])

                client_handles.insert(0, 0)
                valid_tags.insert(0, 0)
                server_handles = []
                errors = []
                
                try:
                    server_handles, errors = opc_items.AddItems(len(client_handles) - 1, valid_tags, client_handles)
                except:
                    pass

                valid_tags_tmp = []
                valid_values_tmp = []
                server_handles_tmp = []
                valid_tags.pop(0)
                
                for i, tag in enumerate(valid_tags):
                    if errors[i] == 0:
                        valid_tags_tmp.append(tag)
                        valid_values_tmp.append(valid_values[i])
                        server_handles_tmp.append(server_handles[i])
                        error_msgs[tag] = ''
                    elif include_error:
                        error_msgs[tag] = self._opc.GetErrorString(errors[i])

                valid_tags = valid_tags_tmp
                valid_values = valid_values_tmp
                server_handles = server_handles_tmp

                server_handles.insert(0, 0)
                valid_values.insert(0, 0)
                errors = []
                
                if len(valid_values) > 1:
                    try:
                        errors = opc_group.SyncWrite(len(server_handles) - 1, server_handles, valid_values)
                    except:
                        pass

                n = 0
                for tag in tags:
                    if tag in valid_tags:
                        if errors[n] == 0:
                            status = 'Success'
                        else:
                            status = 'Error'
                        if include_error:  error_msgs[tag] = self._opc.GetErrorString(errors[n])
                        n += 1
                    else:
                        status = 'Error'

                        
                    #print("iwrite status %s" % (status))
                    # OPC servers often include newline and carriage return characters
                    # in their error message strings, so remove any found.
                    if include_error:  error_msgs[tag] = error_msgs[tag].strip('\r\n')
                    
                    if single:
                        if include_error:
                            result.append((status, error_msgs[tag]))
                        else:
                            result.append(status)
                    else:
                        if include_error:
                            result.append((tag, status, error_msgs[tag]))
                        else:
                            result.append((tag, status))

                opc_groups.Remove(opc_group.Name)
                
            
        except pythoncom.com_error as err:
            print("Error iwrite pythoncom %s" % self._get_error_str(err))
            raise
        except:
            print("Error iwrite %s" % sys.exc_info()[1])
            raise

        return result


    def write(self, tag_value_pairs, size=None, pause=0, include_error=False):
        """Write list of (tag, value) pair(s) to the server"""
        #print("Writing %s with value %s" % (tag_value_pairs[0][0], tag_value_pairs[0][1]))
        
        if type(tag_value_pairs) in (list, tuple) and type(tag_value_pairs[0]) in (list, tuple):
            single = False
        else:
            single = True

        # For simu only
        global simu
        if int(simu) == 1:
            status = write_simu(self, tag_value_pairs, size, pause, include_error)
        else:
            status = self.iwrite(tag_value_pairs, size, pause, include_error)
        

        if single:
            return list(status)[0]
        else:
            return list(status)
        

    def groups(self):
        """Return a list of active tag groups"""
        return self._groups.keys()


    def remove(self, groups):
        """Remove the specified tag group(s)"""

        try:
            pythoncom.CoInitialize()
            opc_groups = self._opc.OPCGroups

            if type(groups) in (str, bytes):
                groups = [groups]
                single = True
            else:
                single = False

            status = []

            for group in groups:
                if group in self._groups:
                    for i in range(self._groups[group]):
                        sub_group = '%s.%d' % (group, i)

                        if sub_group in self._group_hooks:
                            if self.trace: self.trace('CloseEvents(%s)' % sub_group)
                            self._group_hooks[sub_group].close()

                        try:
                            if self.trace: self.trace('RemoveGroup(%s)' % sub_group)
                            errors = opc_groups.Remove(sub_group)
                        except pythoncom.com_error as err:
                            print('RemoveGroup: %s' % self._get_error_str(err))
                            raise

                        del (self._group_tags[sub_group])
                        del (self._group_valid_tags[sub_group])
                        del (self._group_handles_tag[sub_group])
                        del (self._group_server_handles[sub_group])
                    del (self._groups[group])

        except pythoncom.com_error as err:
            print('remove: %s' % self._get_error_str(err))
            raise


    def _get_error_str(self, err):
        """Return the error string for a OPC or COM error code."""

        hr, msg, exc, arg = err.args

        if exc is None:
            error_str = str(msg)
        else:
            scode = exc[5]

            try:
                opc_err_str = unicode(self._opc.GetErrorString(scode)).strip('\r\n')
            except:
                opc_err_str = None

            try:
                com_err_str = unicode(pythoncom.GetScodeString(scode)).strip('\r\n')
            except:
                com_err_str = None

            # OPC error codes and COM error codes are overlapping concepts,
            # so we combine them together into a single error message.

            if opc_err_str == None and com_err_str == None:
                error_str = str(scode)
            elif opc_err_str == com_err_str:
                error_str = opc_err_str
            elif opc_err_str == None:
                error_str = com_err_str
            elif com_err_str == None:
                error_str = opc_err_str
            else:
                error_str = '%s (%s)' % (opc_err_str, com_err_str)

        return error_str
