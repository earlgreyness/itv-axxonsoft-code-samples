# -*- coding: utf-8 -*-

import json
import logging
import os.path
import platform
import time
from datetime import datetime
from decimal import Decimal
import threading
from functools import wraps
from contextlib import contextmanager
from enum import Enum
from operator import itemgetter

import arrow
import requests
from sqlalchemy import (create_engine, Column, Integer,
                        String, DateTime, Float)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

ARCHIVE_EXTENSION = '.afs'

logger = logging.getLogger(__name__)
Base = declarative_base()

#
# Константы:
#

SITUATION_ANALYSIS_DETECTOR = {
    'DetectorModule': 'SituationDetector',
    'DetectorType': 'SceneDescription',
    'ShouldWriteVmdaData': True,
}
LICENSE_PLATES_RECOGNITION_DETECTOR = {
    'DetectorModule': 'LprDetector',
    'DetectorType': 'LprDetector',
    'ShouldWriteVmdaData': True,
}
FACE_DETECTION_DETECTOR = {
    'DetectorModule': 'TvaFaceDetector',
    'DetectorType': 'TvaFaceDetector',
    'ShouldWriteVmdaData': True,
}

TimeSortOrder = Enum('TimeSortOrder', ['NEWER_FIRST', 'OLDER_FIRST'])

class ExportJobState(Enum):
    IN_PROGRESS = 1
    DONE = 2
    ERROR = 3
    NO_SPACE = 4

TIMESTAMP_TOKEN = 'YYYYMMDDTHHmmss.SSS'


#
# Helpers:
#

'''
def check_timestamp(time_stamp):
    """
    Проверят корректность строкового представления отметки времени.
    """
    try:
        arrow.get(time_stamp, ['YYYYMMDDTHHmmss.SSS', 'YYYYMMDDTHHmmss'])
    except arrow.parser.ParserError:
        raise Exception('Time stamp \'{}\' doesn\'t satisfy ISO: YYYYMMDDTHHmmss.SSS'
                        ' or YYYYMMDDTHHmmss'.format(time_stamp))
'''

def arrow_to_ts(arrow_):
    """ timestamp будет получен для UTC """
    return arrow_.to('utc').format('YYYYMMDDTHHmmss.SSS')

def ts_to_arrow(time_stamp):
    """ timestamp трактуется как время по UTC """
    return arrow.get(time_stamp, ['YYYYMMDDTHHmmss.SSS', 'YYYYMMDDTHHmmss'], tzinfo='utc')

def add_int_to_dict(dict_, key, val):
    if val is not None:
        try:
            dict_['key'] = int(val)
        except ValueError:
            raise Exception('Wrong value of {} = {}'.format(key, val))


#
# Классы:
#

class RSGRequestRecord(Base):
    __tablename__ = 'rsg_requests'

    id = Column(Integer, primary_key=True)
    method = Column(String, nullable=False)
    url = Column(String, nullable=False)
    body = Column(String, nullable=False, default='')
    utc_start = Column(DateTime, nullable=False)
    delta = Column(Float, nullable=False)
    status_code = Column(Integer, nullable=False)


def pretty_dict(d):
    return '\n' + json.dumps(d, indent=4, sort_keys=True)


class ServerError(RuntimeError):
    def __init__(self, *args, **kwargs):
        self.json_response = kwargs.pop('json_response', None)
        super(ServerError, self).__init__(*args, **kwargs)


class RSGServerError(ServerError):
    pass


class AxxonObject(object):
    """
    :propery id: Это полное URI объекта, которое выводится в RSG как `id`. К примеру:
                 `"Id": "hosts/AXXON-NODE-NAME/DeviceIpint.3/SourceEndpoint.video:0:0"`.
    """

    def __init__(self, id):
        self.id = id

    def __str__(self):
        return '<{} {}>'.format(type(self).__name__, self.label)

    def __repr__(self):
        return '{}({!r})'.format(type(self).__name__, self.id)

    def __hash__(self):
        return hash('{}{!r}'.format(type(self).__name__, self.id))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def label(self):
        raise NotImplementedError

    @property
    def node(self):
        return self.id.split('/')[1]


class Camera(AxxonObject):

    @classmethod
    def from_display_id(cls, node, display_id, channel=0, stream=0):
        """
        Альтернативный конструкор, создающий объект-обертку над Web API для камеры.

        :param str node: Имя хоста-ноды.
        :param str display_id: DisplayId IP-устройства (при автоматиечком создании камер -- это ее
                               номер).
        :param int channel: Номер канала (нумерация от 0).
        :param int stream: Номер потока в канале (нумерация от 0).
        """
        id = 'hosts/{}/DeviceIpint.{}/SourceEndpoint.video:{}:{}'.format(
            node, display_id, channel, stream)
        return cls(id)

    @property
    def video_source_id(self):
        """
        Возвращает только VideoSourceID. К примеру: `AXXON-NODE-NAME/DeviceIpint.3/SourceEndpoint.video:0:0`.
        :return type: str
        """
        return '/'.join(self.id.split('/')[1:])

    @property
    def label(self):
        return self.id.split('/')[2].split('.')[1]

    @property
    def display_id(self):
        return int(self.id.split('/')[2].split('.')[1])

    @property
    def source_endpoint_id(self):
        return '{}/SourceEndpoint.video:0:0'.format(self.id)

    @property
    def embedded_storage_id(self):
        return '{}/MultimediaStorage.0'.format(self.id)


class Archive(AxxonObject):
    @property
    def label(self):
        return self.id.split('/')[2].split('.')[1]


class Detector(AxxonObject):
    def __init__(self, id, camera, name=None):
        self.endpoint = None
        if id.endswith('EventSupplier'):
            pass
        elif id.endswith('SourceEndpoint.vmda'):
            self.endpoint = id
            id = id[:id.rindex('SourceEndpoint.vmda')] + 'EventSupplier'
        else:
            raise
        super(Detector, self).__init__(id)
        self.camera = camera
        self.name = name

    @property
    def label(self):
        n = self.id.split('/')[2].split('.')[1]
        return '{} "{}" for Camera {}'.format(n, self.name, self.camera.label)

    def get_id_for_search(self, vmda=False):
        parts = self.id.split('/')
        if vmda:
            cut = parts[1:3] + ['SourceEndpoint.vmda']
        else:
            cut = parts[1:]
        return '/'.join(cut)


class Connection(requests.Session):
    def __init__(self, addr='localhost', port=None, auth=('root', 'root'), prefix=None):
        """
        :param str prefix: Префик путей к Web-ресурсам: http://<addr>[:port][/prefix]/... Если None
                           или пустая строка, то считается, что префикса нет.
        """
        super(Connection, self).__init__()
        assert port is not None
        self.auth = auth
        self.addr = addr
        self.port = port
        self.prefix = prefix

    @staticmethod
    def check_for_error(r):
        raise Exception('Mehtod Connection.check_for_error(...) must be overridden')

    def prepare(self, f):
        @wraps(f)
        def tmp(self, path, **kwargs):
            assert path.startswith('/')
            r = f(self, self.base_url + path, **kwargs)
            self.check_for_error(r)
            return r
        return tmp

    @property
    def base_url(self):
        base_url = 'http://{}:{}'.format(self.addr, self.port)
        if self.prefix:
            base_url += '/{}'.format(self.prefix)
        return base_url

    def get(self, path, **kwargs):
        return self.prepare(requests.Session.get)(self, path, **kwargs)

    def post(self, path, **kwargs):
        return self.prepare(requests.Session.post)(self, path, **kwargs)

    def put(self, path, **kwargs):
        return self.prepare(requests.Session.put)(self, path, **kwargs)

    def delete(self, path, **kwargs):
        return self.prepare(requests.Session.delete)(self, path, **kwargs)


class WebHttpApi(Connection):

    @staticmethod
    def check_for_error(r):
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                j = r.json()
            except ValueError:
                j = {}
            msg = (
                '\nStatus code: {}\nHeaders: {}\n'
                'Text: {}\nContent: {}\n'
                'JSON: {}\n'.format(r.status_code,
                                    pretty_dict(dict(r.headers)),
                                    r.text,
                                    r.content,
                                    pretty_dict(j))
            )
            raise ServerError(msg)

    def get_nodes(self):
        return self.get('/hosts').json()

    def get_cpu_load(self):
        j = self.get('/statistics/hardware').json()
        return float(j[0]['totalCPU'].replace(',', '.')) / 100

    def get_arch_intervals(self, display_id, node=None, channel=0, stream=0,
                           begin_time=None, end_time=None, limit=None, scale=None,
                           sort_order=TimeSortOrder.NEWER_FIRST):
        """
        Получение списка интервалов в архиве.
        https://doc.axxonsoft.com/confluence/pages/viewpage.action?pageId=115607678

        TODO: порядок сортировки интервалов при limit

        :param display_id: DisplayId
        :param display_id type: str или int
        :param str node: Имя хоста-ноды. Если не указано, везьмем то, которое вернет запрос get_nodes()
                         в вслучае односерверного домена.
        :param int channel: Номер канала IP-устройства.
        :param int stream: Номер потока в канале `channel`.
        :param end_time: Конец отрезка времени, на котором ищутся инетрвалы в архиве (в Web API
                         используется UTC).
        :param end_time type: :class:`Arrow`
        :param begin_time: Начало отрезка времени, на котором ищутся инетрвалы в архиве (в Web API
                           используется UTC).
        :param begin_time type: :class:`Arrow`
        :param int limit: Максимальное число интервалов в ответе на Web-запрос.
        :param int scale: Минимальное значение разрыва между двумя интервалами в архиве, при котором
                          эти интервалы не будут объединиться в один. Если разрыв меньше `scale`, то
                          интервалы-записи в архиве скливаются.
        :param sort_order: Как будет осортирован список интервалов.
        :param sort_order: :class:`TimeSortOrder`

        TimeSortOrder = Enum('TimeSortOrder', ['NEWER_FIRST', 'OLDER_FIRST'])

        :return: Возвращает структуру из интервалов и флаг more. Все отметки времени являются
                 объектами :class:`Arrow`.
        :return type: two items tuple
        """
        if node is None:
            nodes = self.get_nodes()
            if len(nodes) != 1:
                raise Exception('Cann\'t choose node name automatically: get_nodes() returns {}'.format(
                    nodes))
            node = nodes[0]

        params = {}
        add_int_to_dict(params, 'limit', limit)
        add_int_to_dict(params, 'scale', scale)

        if end_time is None:
            end_time = 'future'
        else:
            end_time = arrow_to_ts(end_time)

        if begin_time is None:
            begin_time = 'past'
        else:
            begin_time = arrow_to_ts(begin_time)

        cam = Camera.from_display_id(node, display_id)
        r = self.get(
            '/archive/contents/intervals/{}/{}/{}'.format(cam.video_source_id, end_time, begin_time),
            params=params)
        data = r.json()
        data['intervals'] = [
            {'begin': ts_to_arrow(i['begin']), 'end': ts_to_arrow(i['end'])}
            for i in data['intervals']
        ]
        return (sorted(data['intervals'], key=itemgetter('begin'),
                       reverse=(sort_order is TimeSortOrder.NEWER_FIRST)),
                data['more'])

    def export_video(self, display_id, begin_time, end_time,
                     format_, save_dir, node=None, timeout=None):
        """
        https://doc.axxonsoft.com/confluence/pages/viewpage.action?pageId=133530728

        :param str begin_time: Время начала экспорта (в Web API используется UTC).
        :param str end_time: Время конца экспорта (в Web API используется UTC).
        :param str save_dir: Директория, куда будут складываться файлы.
        """
        class logger:
            @staticmethod
            def debug(a):
                print a

        format_ = str(format_).lower()
        if format_ not in ['mkv', 'avi', 'exe', 'jpg', 'pdf']:
            raise Exception('Unsupported export format_ = {}'.format(format_))
        payload = {'format': format_}
        begin_time = arrow_to_ts(begin_time)
        end_time = arrow_to_ts(end_time)

        if not os.path.isdir(save_dir):
            os.remove(save_dir)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        cam = Camera.from_display_id(node, display_id)
        r = self.post(
            '/export/archive/{}/{}/{}'.format(cam.video_source_id, begin_time, end_time),
            data=json.dumps(payload))
        job_id = r.headers['Location']
        logger.debug('Export job_id = \'{}\' from url = \'{}\''.format(job_id, r.url))

        if timeout is not None:
            raise Exception('TODO')
        sleep = 1
        acc_sleep = 0
        while True:
            # TODO: сделать таймуат завершения процедуры. С кореектной прворкой отмены экспорта!
            r = self.get('{}/status'.format(job_id))
            resp_dict = r.json()
            if resp_dict.get('state', None) != ExportJobState.IN_PROGRESS.value:
                break
            try:
                p = float(resp_dict['progress'])
                sleep = (1.0-p)/p * acc_sleep / 2.0
            except:
                pass
            finally:
                if sleep < 1 or sleep > 30:
                    sleep = 1
            logger.debug('Export state of job_id = \'{}\': progress = {}; next sleep = {}; acc_sleep = {}'.format(job_id, resp_dict['progress'], sleep, acc_sleep))
            time.sleep(sleep)
            acc_sleep += sleep

        if resp_dict['state'] is not ExportJobState.DONE.value:
            raise Exception('Error during export: \"{error}\" (code={state}).'
                            ' Progress: {progress}.'.format(**resp_dict))
        files = resp_dict['files']
        logger.debug('Export job_id = \'{}\' has been finished: files = {}'.format(job_id, files))

        file_pathes = []
        for file_name in files:
            r = self.get('{}/file?name={}'.format(job_id, file_name), stream=True)
            file_pathes.append(os.path.join(save_dir, file_name))
            with open(file_pathes[-1], 'wb') as fd:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    fd.write(chunk)

        logger.debug('Export job_id = \'{}\' files have been saved to {}'.format(job_id, file_pathes))
        return file_pathes


class RsgHttpApi(Connection):
    def __init__(self, *args, **kwargs):
        log_db = kwargs.pop('log_db', None)
        super(RsgHttpApi, self).__init__(*args, **kwargs)
        if log_db is not None:
            full_path = os.path.abspath(os.path.normpath(log_db))
            engine = create_engine('sqlite:///{}'.format(full_path))
            Base.metadata.create_all(bind=engine)
            Session = sessionmaker(bind=engine)
            self.db_session = scoped_session(Session)
        else:
            self.db_session = None

        self.objects_functions = {
            'Camera': self.get_cameras,
            'Archive': self.get_archives,
            'Detector': self.get_detectors,
        }
        self.objects = self.objects_functions.keys()

        self.locks = {name: threading.Lock() for name in self.objects}


    def prepare(self, f):
        @wraps(f)
        def tmp(self, path, **kwargs):
            logger.debug('{} {}'.format(path, kwargs))
            assert path.startswith('/')
            start = datetime.utcnow()
            r = f(self, self.base_url + path, **kwargs)
            if self.db_session is not None:
                try:
                    data = {
                        'method': r.request.method,
                        'url': r.request.url,
                        'body': r.request.body,
                        'utc_start': start,
                        'delta': r.elapsed.total_seconds(),
                        'status_code': r.status_code,
                    }
                    self.db_session.add(RSGRequestRecord(**data))
                    self.db_session.commit()
                except Exception as e:
                    logger.error('Error working with DB: {}'.format(e))
            self.check_for_error(r)
            return r
        return tmp

    @contextmanager
    def get_new_object(self, object_class):
        """
        Метод-фабрика. Дважды (до и после создания) через API запрашивается список объектов, что
        позволяет проверить факт создания нового объекта.
        """
        class Container(object):
            pass
        a = Container()
        name = object_class.__name__
        function = self.objects_functions[name]
        with self.locks[name]:
            objects_before = function()
            yield a
            objects_after = function()
        objects_new = list(set(objects_after) - set(objects_before))
        assert len(objects_new) == 1
        a.created_object = objects_new[0]

    @staticmethod
    def check_for_error(r):
        j = r.json()
        ignored_messages = [
            "Can't find objects to delete",
            "Can't find detectors to remove",
            "Nothing to flush, no operations were performed",
        ]
        if (j['Result'] != 'Success' and
                j['Message'] not in ignored_messages):
            msg = (
                '\nStatus Code: {}\nHeaders: {}\n'
                'Server json response: {}'.format(r.status_code,
                                                  pretty_dict(dict(r.headers)),
                                                  pretty_dict(j))
            )
            raise RSGServerError(msg, json_response=j)

    @staticmethod
    def fix_drive_letter_case(path):
        norm = os.path.abspath(os.path.normpath(path))
        if platform.system() == 'Windows':
            fixed_path = norm[:1].upper() + norm[1:]
        else:
            fixed_path = norm
        return fixed_path

    def create_camera(self, data):
        INI_KEYS = ('Vendor', 'Model')
        ini_data = {k: data[k] for k in INI_KEYS if k in data}
        upd_data = {k: data[k] for k in data if k not in INI_KEYS}
        with self.get_new_object(Camera) as a:
            self.post('/rsg/ipint', json=ini_data)
        camera = a.created_object
        self.put('/rsg/ipint', json=upd_data, params={'id': camera.id})
        self.flush()
        logger.info("{} created.".format(camera))
        return camera

    def create_virtual_camera(self, video_clips_folder=None):
        data = {
            'Vendor': 'AxxonSoft',
            'Model': 'Virtual',
        }
        if video_clips_folder is not None:
            data['vstream-virtual/folder'] = self.fix_drive_letter_case(video_clips_folder)
        return self.create_camera(data)

    def create_archive(self, archive_file, size=5,
                       should_format=True, color='Red'):
        # archive_size is in GB.
        if not should_format:
            size = 0
        path = self.fix_drive_letter_case(archive_file)
        volume = '{}|{}|{}'.format(
            path, size, ('true' if should_format else 'false'))
        name = os.path.basename(path)
        if name.endswith(ARCHIVE_EXTENSION):
            name = name[:-len(ARCHIVE_EXTENSION)]
        data = {
            'Volumes': volume,
            'Name': name,
            'Color': color,
        }
        with self.get_new_object(Archive) as a:
            self.post('/rsg/archive', json=data)
        arch = a.created_object
        self.flush()
        logger.info('{} created.'.format(arch))
        logger.debug(pretty_dict(self.get_info(arch)))
        return arch

    def create_detector(self, data, camera):
        """
        :returns str: Строка вида "hosts/SERVER/AVDetector.1/EventSupplier". При этом, короткая
                      команда `prepareImport` возвращает строку вида
                      "hosts/SERVER/AVDetector.1/SourceEndpoint.vmda"! Важно это помнить.
        """
        INI_KEYS = ('DetectorModule', 'DetectorType')
        ini_data = {k: data[k] for k in INI_KEYS if k in data}
        upd_data = {k: data[k] for k in data if k not in INI_KEYS}
        with self.get_new_object(Detector) as a:
            self.post('/rsg/detector', json=ini_data, params={'pid': camera.source_endpoint_id})
        detector = a.created_object
        upd_id = '{0}|{1}'.format(camera.id, detector.id)
        self.put('/rsg/detector', json=upd_data, params={'id': upd_id})
        self.flush()
        logger.info('{} created ({}).'.format(detector, camera))
        logger.debug(pretty_dict(self.get_info(detector)))
        return detector

    def update_detector(self, detector, data):
        upd_id = '{0}|{1}'.format(detector.camera.id, detector.id)
        self.put('/rsg/detector', json=data, params={'id': upd_id})
        self.flush()
        logger.info('{} updated.'.format(detector))
        logger.debug(pretty_dict(self.get_info(detector)))

    def get_camera(self, display_id):
        cameras = [c for c in self.get_cameras() if c.display_id == int(display_id)]
        assert len(cameras) == 1
        return cameras[0]

    get_camera_by_id = get_camera

    def get_cameras(self):
        _list = self.get('/rsg/ipint').json()['Data']
        return [Camera(item['Id']) for item in _list]

    def get_archives(self):
        _list = self.get('/rsg/archive').json()['Data']
        return [Archive(item['Name']) for item in _list]

    def get_detectors(self):
        detectors = []
        for c in self.get('/rsg/detector').json()['Data']:
            camera = Camera(c['Id'])
            for ch in c['Children']:
                id = ch['Id']
                name = ch['Settings']['DisplayName']
                detectors.append(Detector(id, camera, name=name))
        return detectors

    def delete_vmda_data(self, camera):
        self.delete('/rsg/vmda/data', params={'id': camera.id})
        self.flush()
        logger.info('VMDA data for {} deleted.'.format(camera))

    def delete_camera(self, camera, remove_vmda_data=True):
        if remove_vmda_data:
            self.delete_vmda_data(camera)
        self.delete('/rsg/ipint', params={'id': camera.id})
        self.flush()
        logger.info('{} deleted.'.format(camera))

    def delete_all_cameras(self):
        self.delete('/rsg/ipint', params={'id': '.'})
        self.flush()
        logger.info("All cameras deleted.")

    def delete_all_archives(self):
        self.delete('/rsg/archive', params={'id': '.'})
        self.flush()
        logger.info("All archives deleted.")

    def delete_all_detectors(self):
        self.delete('/rsg/detector', params={'id': '.'})
        self.flush()
        logger.info("All detectors deleted.")

    def flush(self):
        self.post('/rsg', json={'action': 'flush'})

    def bind_camera_to_archive(self, camera, archive, permanent_write=False,
                               replication=False):
        params = {
            'id': camera.source_endpoint_id,
            'pid': archive.id,
        }
        data = {
            'Bind': camera.source_endpoint_id,
            'PermanentWrite': permanent_write,
        }
        if replication:
            data['SourceArchive'] = camera.embedded_storage_id
        self.post('/rsg/binding', json=data, params=params)
        self.flush()
        word = 'permanent write' if permanent_write else 'on-demand'
        if replication:
            logger.info('Embedded storage of {} '
                        'bound to {} ({} replication).'.format(camera, archive, word))
        else:
            logger.info('{} bound to {} ({} recording).'.format(camera, archive, word))

    def start_import(self, camera, archive, begin_time, end_time):
        logger.info('Start import: begin_time = {}, end_time = {}).'.format(begin_time, end_time))
        data = {
            'Archive': archive.id,
            'BindingName': camera.source_endpoint_id,
            'BeginTime': int(begin_time),
            'EndTime': int(end_time),
        }
        j = self.post('/rsg/binding/replication', json=data).json()
        self.flush()
        return j['Data']['Token']

    def get_info(self, obj):
        if isinstance(obj, Camera):
            res = self.get('/rsg/ipint?id="{}"'.format(obj.id)).json()['Data']
        elif isinstance(obj, Archive):
            res = self.get('/rsg/archive?id="{}"'.format(obj.id)).json()['Data']
        elif isinstance(obj, Detector):
            # TODO!!! Временное рещение, см. ACR-29213
            res = self.get('/rsg/detector?id="{}|{}"'.format(obj.camera.id, obj.id)).json()['Data']
        if len(res) > 1:
            logger.error('len(res) > 1\nrequested object: {} (id = {})\nres:{}'.format(
                obj, obj.id, pretty_dict(res)))
        return res

    def get_import_progress(self, token):
        j = self.get('/rsg/binding/replication', params={'id': token}).json()
        return float(j['Data']['Progress']) / 100

    def print_cameras_info(self):
        logger.info(pretty_dict(self.get('/rsg/ipint').json()['Data']))

    def print_archives_info(self):
        logger.info(pretty_dict(self.get('/rsg/archive').json()['Data']))

    def print_detectors_info(self):
        logger.info(pretty_dict(self.get('/rsg/detector').json()['Data']))

# data = {
#     'Action': 'prepareImport',
#     'ServiceAddress': ECHD_SERVER,
#     'DeviceGuid': GUID,
#     'Login': USER,
#     'Password': PASSWORD,
#     'ArchiveSize': 1000,
#     'Port': 8088,
# }
# logger.info(api.post('/rsg', json=data).json())

