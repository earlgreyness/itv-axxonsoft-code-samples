# -*- coding: utf-8 -*-

import traceback
import os.path
import os
import re
import sys
import time
from StringIO import StringIO
import logging
from logging import (Filter, StreamHandler, FileHandler,
                     NullHandler, Formatter, Handler)

try:
    import robot.api.logger
    from robot.running.context import EXECUTION_CONTEXTS
except ImportError:
    pass

from tcp_messages_socket import TCPMessagesSocket

LOGGING_HOST = '192.168.116.1'
LOGGING_PORT = 7778
LOCAL_LOG_FILE = 'D:\\Reports\\axxon-autotest.log'


def escape_html(html):
    # Replaces order is critically important.
    rules = [
        ('&', '&amp;'),
        ('<', '&lt;'),
        ('>', '&rt;'),
    ]
    for rule in rules:
        html = html.replace(*rule)
    return html


class RFListenerExclusionFilter(Filter):
    def filter(self, record):
        return 'rf_listener' not in record.name


class RelevantMessagesFilter(Filter):
    pass


class RobotFrameworkHandler(StreamHandler):
    RF_LOG_LEVELS = {
        logging.DEBUG: 'DEBUG',
        logging.INFO: 'INFO',
        logging.WARNING: 'WARN',
        logging.ERROR: 'ERROR',
        logging.CRITICAL: 'ERROR',
    }

    def __init__(self):
        StreamHandler.__init__(self, sys.stdout)

    def write_record_to_robot(self, record):
        # self.formatter is going to be ignored.
        # We send to RF the raw logged message.
        # All metadata is therefore lost.
        # However, RF inserts its own timestamps,
        # and also we do provide log level
        # information, which is ehough for html report.
        msg = record.getMessage()
        msg = escape_html(msg)
        msg = self.mark_filenames_with_html(msg, record.f)

        level = self.RF_LOG_LEVELS.get(record.levelno, 'DEBUG')
        try:
            robot.api.logger.write(msg, level=level, html=True)
        except NameError:
            # In case robot.api.logger was not imported.
            pass

    def is_robot_running(self):
        try:
            return EXECUTION_CONTEXTS.current is not None
        except NameError:
            # In case EXECUTION_CONTEXTS was not imported.
            # It means there is not Robot Framework installed.
            return False

    def emit(self, record):
        try:
            files = getattr(record, 'f', [])
            try:
                iter(files)
            except TypeError:
                files = [files]
            setattr(record, 'f', files)

        except Exception as e:
            sys.__stdout__.write('Exception occured [#1]: {}'.format(e) + '\n')
            sys.__stdout__.flush()

        else:
            try:
                if self.is_robot_running():
                    # We end up here only if within this
                    # Python process a Robot Framework
                    # test suite is running, and we are
                    # inside an execution context.
                    self.write_record_to_robot(record)

                elif sys.stdout is not sys.__stdout__:
                    for f in record.f:
                        record.msg = record.msg.replace('[[f]]', f.name_for_plain(), 1)
                    self.stream = sys.stdout
                    StreamHandler.emit(self, record)
            except Exception as e:
                sys.__stdout__.write('Exception occured [#2]: {}'.format(e) + '\n')
                sys.__stdout__.flush()
                # Logging operations must be silent.
                pass

    @classmethod
    def custom_replace(cls, match):
        # match.group() extracts full RE match as a string.
        m = os.path.normpath(match.group())
        # r'\g<0>'
        return ('<a href="#" class="PIKULI_pattern_file_name">{0}'
                '<span class="PIKULI_pattern_preview">'
                '<img src="/pikuli-image?filename={0}">'
                '</span></a>'.format(m))

    @classmethod
    def mark_filenames_with_html(cls, msg, files):
        #pattern = \
        #    r'([a-zA-Z]:\\).+?\.(png|bmp|jpg|jpeg|PNG|BMP|JPG|JPEG)'
        #return re.sub(pattern, cls.custom_replace, msg)
        for f in files:
            msg = msg.replace('[[f]]', f.name_for_html(), 1)
        return msg


class AxxonSocketHandler(Handler):
    def __init__(self, host, port):
        Handler.__init__(self)
        self.host = host
        self.port = port
        self.timeout = 5.0  # sec
        self._p2c_tcp_conn = TCPMessagesSocket()
        # self._connect() is called when
        # first record is emitted. So it's lazy.

    def emit(self, record):
        try:
            if not self._p2c_tcp_conn.is_connected():
                self._connect()
            msg = self.format(record)
            if record.levelno in (logging.CRITICAL, logging.ERROR):
                content_type = 'exception'
            else:
                content_type = 'text'
                msg += '\n'
            self._p2c_tcp_conn.send_msg({content_type: str(msg)})

        except Exception as e:
            sys.__stdout__.write('*** ERROR in AxxonSocketHandler.emit: %s\n' % str(e))
            sys.__stdout__.write(''.join(traceback.format_list(traceback.extract_tb(sys.exc_info()[2])[1:])) + '\n\n')
            sys.__stdout__.flush()
            try:
                self._p2c_tcp_conn.disconnect()
            except Exception:
                pass

    def _connect(self):
        try:
            self._p2c_tcp_conn.connect(self.host, self.port, self.timeout,
                                       exception_of_fail=False)
        except Exception:
            pass


class PrependingFilter(Filter):
    def __init__(self, phrase):
        Filter.__init__(self)
        self.phrase = phrase

    def filter(self, record):
        try:
            record.msg = self.phrase + str(record.msg)
        except Exception:
            # Filter does nothing.
            pass
        return True


class StreamToLogger(StringIO):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger):
        StringIO.__init__(self)
        self.logger = logger

    def write(self, buf):
        StringIO.write(self, buf)
        if '\n' in buf:
            for line in self.getvalue().splitlines():
                self.logger.info(line.rstrip())
            self.clean_buffer()

    def clean_buffer(self):
        StringIO.__init__(self)


def configure_loggers():

    logger = logging.getLogger('axxon')
    logger_alt = logging.getLogger('axxonnext')

    if logger.handlers:
        # logger already has handlers. It can only mean
        # that the logger has already been configured
        # within the current Python interpreter process
        # by this function.
        logger.debug('"axxon" loggers already configured!')
        return

    debug_f = Formatter('[%(asctime)s] [%(levelname)s] %(name)s %(message)s')
    simple_f_1 = Formatter('[%(levelname)s] %(name)s %(message)s')
    simple_f_2 = Formatter('%(asctime)s.%(msecs).03d [%(levelname)s] %(name)s %(message)s',
                           datefmt='%H:%M:%S')

    try:
        fi = os.environ.get('AXXON_AUTOTEST_REPORTS_DIR', LOCAL_LOG_FILE)
        handler_local_file = FileHandler(fi, encoding='utf-8')
    except Exception:
        # Permission, OS, filesystem, not found errors.
        handler_local_file = NullHandler()
    handler_local_file.setFormatter(debug_f)

    handler_real_stdout = StreamHandler(sys.__stdout__)
    handler_real_stdout.setFormatter(simple_f_1)

    handler_robot = RobotFrameworkHandler()
    handler_robot.setFormatter(simple_f_2)
    handler_robot.addFilter(RFListenerExclusionFilter())

    try:
        handler_socket = AxxonSocketHandler(LOGGING_HOST, LOGGING_PORT)
        handler_socket.setFormatter(simple_f_2)
    except Exception as e:
        sys.__stdout__.write('*** ERROR: Cann\'t configure AxxonSocketHandler: {}'.format(e))
        sys.__stdout__.flush()
        handler_socket = NullHandler()

    for item in (logging.getLogger(name) for name in ['axxon', 'axxonnext']):
        item.setLevel(logging.DEBUG)
        item.addHandler(handler_local_file)
        item.addHandler(handler_real_stdout)
        item.addHandler(handler_robot)
        item.addHandler(handler_socket)
        item.propagate = False
        item.addFilter(RelevantMessagesFilter())

    logging.getLogger('axxon').debug('"axxon" and "axxonnext" loggers configured')


def testing_function():
    logging.getLogger('axxon').info('Logger called from axxon_autotest_loggers.py')


if __name__ == '__main__':
    class CustomFilter(Filter):
        def filter(self, record):
            print('INSIDE')
            msg = record.msg
            print(msg)
            # print(record.message)
            return False

    logger = logging.getLogger('kirill')
    logger.addFilter(CustomFilter())
    logger.addHandler(StreamHandler(sys.stdout))

    logger.warning('Whatever %s', 12)

