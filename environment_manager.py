# -*- coding: utf-8 -*-

import platform
import psutil
import shutil
import logging
from decimal import Decimal
import os
import os.path
import subprocess
from collections import OrderedDict
import time
import arrow
import inspect

logger = logging.getLogger(__name__)

B_IN_KB = 1024
B_IN_MB = B_IN_KB * 1024
COMPANY = 'AxxonSoft'
PRODUCT = 'AxxonNext'
LOC_DIR = os.path.join(os.environ['LOCALAPPDATA'],    COMPANY, PRODUCT)
ALL_DIR = os.path.join(os.environ['ALLUSERSPROFILE'], COMPANY, PRODUCT)
PRO_DIR = os.path.join(os.environ['ProgramFiles'],    COMPANY, PRODUCT)
DEFAULT_CONFIG = OrderedDict([
    ('SERVER_PROCESS_PARENT', 'AppHostSvc.exe'),
    ('SERVER_PROCESS_CHILD', 'AppHost.exe'),
    ('CLIENT_PROCESS', 'AxxonNext.exe'),
    ('RSG_PROCESS', 'rsg.exe'),
    ('POSTGRES_PROCESS', 'postgres.exe'),
    ('SERVER_PROCESS_NET', 'ngp_host_service'),
    ('RSG',           os.path.join(PRO_DIR, 'bin', 'rsg.exe')),
    ('BIN',           os.path.join(PRO_DIR, 'bin')),
    ('VMDA',          os.path.join(PRO_DIR, 'Metadata', 'vmda_db')),
    ('CONFIG_LOCAL',  os.path.join(ALL_DIR, 'Config.local')),
    ('CONFIG_SHARED', os.path.join(ALL_DIR, 'Config.shared')),
    ('LOGS_SERVER',   os.path.join(ALL_DIR, 'Logs')),
    ('LOGS_CLIENT',   os.path.join(LOC_DIR, 'Logs')),
])


class Manager(object):
    def __init__(self, config=None):
        self.config = DEFAULT_CONFIG
        if config is not None:
            self.config.update(config)

    def restore_config(self, folder):
        pass

    @staticmethod
    def _kill_process_by_name(*names):
        processes = (proc for proc in psutil.process_iter() if proc.name() in names)
        for process in processes:
            logger.debug('Process: {}'.format(process))
            for child in process.children(recursive=True):
                logger.debug('  subprocess: {}'.format(child))
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass

    def start_rsg(self, **kwargs):
        logger.debug('Starting RSG in HTTP server mode...')
        exe = self.config['RSG']
        args = '--host={node} --http-port={api_port} -log=TRACE'.format(**kwargs)
        subprocess.Popen([exe] + args.split())
        logger.debug('RSG HTTP API started.')

    def stop_rsg(self):
        logger.debug('Stopping RSG...')
        self._kill_process_by_name(self.config['RSG_PROCESS'])
        logger.debug('RSG stopped.')

    def start_client(self):
        logger.debug('Starting client...')
        client_path = os.path.join(self.config['BIN'],
                                   self.config['CLIENT_PROCESS'])
        _ = subprocess.Popen(client_path)
        logger.debug('Client started.')

    def kill_client(self):
        logger.debug('Killing client...')
        self._kill_process_by_name(self.config['CLIENT_PROCESS'])
        logger.debug('Client killed.')

    def is_client_running(self):
        return bool([proc for proc in psutil.process_iter()
                     if proc.name() == self.config['CLIENT_PROCESS']])

    def start_server(self):
        logger.debug('Starting server...')
        command = 'NET START {}'.format(self.config['SERVER_PROCESS_NET'])
        output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        logger.debug('Server started.')

    def stop_server(self):
        logger.debug('Stopping server...')
        command = 'NET STOP {}'.format(self.config['SERVER_PROCESS_NET'])
        output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        logger.debug('Server stopped.')

    def kill_server(self):
        logger.debug('Killing server...')
        self._kill_process_by_name(self.config['SERVER_PROCESS_PARENT'])
        logger.debug('Server killed.')

    def is_server_running(self):
        return bool([proc for proc in psutil.process_iter()
                     if proc.name() == self.config['SERVER_PROCESS_PARENT']])

    def wait_for_server_stop(self, timeout):
        t0 = arrow.now()
        while (arrow.now() - t0).seconds <= timeout:
            if not self.is_server_running():
                return True
            time.sleep(1)
        return False

    def wait_for_server_start(self, timeout):
        t0 = arrow.now()
        while (arrow.now() - t0).seconds <= timeout:
            if self.is_server_running():
                return True
            time.sleep(1)
        return False

    @staticmethod
    def cpu_load():
        try:
            return psutil.cpu_percent(interval=0.1) / 100
        except Exception:
            logger.exception('Could not get cpu_load.')
            return 0.0

    def axxon_server_ram_usage(self):
        FATHER = self.config['SERVER_PROCESS_PARENT']
        CHILD = self.config['SERVER_PROCESS_CHILD']
        if platform.system() != 'Windows':
            raise NotImplementedError('Windows only.')
        procs = [p for p in psutil.process_iter()
                 if p.name().lower() == FATHER.lower()]
        ram_usage = 0
        if len(procs) > 1:
            logger.warning('{} instances of "{}" running '
                           '(only one expected).'.format(len(procs), FATHER))
        for proc in procs:
            for ch in proc.children(recursive=True):
                try:
                    ram_usage += ch.memory_info_ex().private
                except psutil.NoSuchProcess:
                    pass
                except AttributeError as ex:
                    code = inspect.currentframe().f_code
                    logger.error(str(ex) + '; see function {!r} in file {!r}'.format(
                        code.co_name, code.co_filename))
        return ram_usage / B_IN_MB

    def postgres_ram_usage(self):
        if platform.system() != 'Windows':
            raise NotImplementedError('Windows only.')
        procs = [p for p in psutil.process_iter()
                 if p.name().lower() == self.config['POSTGRES_PROCESS'].lower()]
        ram_usage = 0
        for p in procs:
            try:
                ram_usage += p.memory_info_ex().private
            except psutil.NoSuchProcess:
                pass
            except AttributeError as ex:
                code = inspect.currentframe().f_code
                logger.error(str(ex) + '; see function {!r} in file {!r}'.format(
                    code.co_name, code.co_filename))
        return ram_usage / B_IN_MB

    @staticmethod
    def calc_folder_size(path):
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total_size += os.path.getsize(fp)
                    except OSError as e:
                        # In case, for instance, when
                        # a file has already been removed since
                        # os.walk has listed folder contents.
                        logger.warning('{}: {}'.format(type(e).__name__, e))
        except Exception:
            logger.exception('Could not get folder size.')
        return float(total_size / B_IN_MB)

    def vmda_size(self):
        return self.calc_folder_size(self.config['VMDA'])

    def get_all_dmp_files(self):
        fi = []
        try:
            for folder in (self.config['LOGS_CLIENT'], self.config['LOGS_SERVER']):
                for dirpath, dirnames, filenames in os.walk(folder):
                    for f in filenames:
                        fp = os.path.join(dirpath, f)
                        if os.path.basename(fp).endswith('.dmp'):
                            fi.append(fp)
        except Exception:
            logger.exception('Could not list all .dmp files.')
        return fi

    def config_size(self, local=True, shared=True):
        assert local or shared
        loc = self.config['CONFIG_LOCAL']
        sha = self.config['CONFIG_SHARED']
        size = 0
        if local:
            size += self.calc_folder_size(loc)
        if shared:
            size += self.calc_folder_size(sha)
        return size

    def delete_all_logs(self):
        for folder in (self.config['LOGS_SERVER'], self.config['LOGS_CLIENT']):
            for the_file in os.listdir(folder):
                path = os.path.join(folder, the_file)
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                except Exception:
                    pass
