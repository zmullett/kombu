from functools import wraps, partial as curry

import anyjson

from tornado import httpserver
from tornado import ioloop
from tornado.web import Application
from tornado.web import RequestHandler, HTTPError

from kombu.utils import partition
from kombu.connection import BrokerConnection

DEFAULT_PORT = 9812


def message_to_dict(m):
    return {"body": m.body,
            "content_type": m.content_type,
            "content_encoding": m.content_encoding,
            "headers": m.headers,
            "properties": m.properties}


class AMQHandler(RequestHandler):
    _amqchannel = None

    def arg(self, name, default, *types):
        if name[0] == "!": # required
            value = self.get_argument(name[1:])
        else:
            value = self.get_argument(name, None)

        for type in types:
            value = type(value)

        if value is None:
            return default
        return value

    def write(self, chunk):
        if not isinstance(chunk, basestring):
            self.set_header("Content-Type",
                            "application/javascript; charset=UTF-8")
            chunk = anyjson.serialize(chunk)
        super(AMQHandler, self).write(chunk)

    def args(self, *argtup):
        return dict((name, self.arg(name, default, *types))
                        for name, default, types in argtup)

    def get_content_type(self):
        t = self.request.headers["Content-Type"]
        content_type, _, metastr = partition(t, ";")
        meta = dict((key.lower(), value)
                        for key, _, value in map(
                            curry(partition, sep="="), metastr.split()))
        return content_type.strip(), meta.get("charset", "utf-8").lower()

    @property
    def amqchannel(self):
        if self._amqchannel is None:
            self._amqchannel = self.application.broker.channel()
        return self._amqchannel


def maybe_int(i):
    try:
        return int(i)
    except (TypeError, ValueError):
        return i


def maybe_bool(b):
    if b is None:
        return False
    if b.lower() == "false":
        return False
    return bool(maybe_int(b))


class queue_declare(AMQHandler):

    def post(self, queue):
        kwargs = self.args(("passive", False, (maybe_bool, )),
                           ("durable", True, (maybe_bool, )),
                           ("exclusive", False, (maybe_bool, )),
                           ("auto_delete", False, (maybe_bool, )))
                           # TODO arguments
        self.write(self.amqchannel.queue_declare(queue=queue, **kwargs))


class queue_bind(AMQHandler):

    def post(self, queue, exchange):
        kwargs = self.args(("routing_key", "", (str, )))
                           # TODO arguments
        self.write(self.amqchannel.queue_bind(queue=queue,
                                              exchange=exchange,
                                              **kwargs))


class exchange_declare(AMQHandler):

    def post(self, exchange):
        kwargs = self.args(("type", "direct", (str, )),
                           ("durable", True, (maybe_bool, )),
                           ("auto_delete", False, (maybe_bool, )))
                           # TODO arguments
        try:
            self.write(self.amqchannel.exchange_declare(exchange=exchange,
                                                        **kwargs))
        except KeyError, entity:
            raise HTTPError(404, "Unknown entity: %s" % (entity, ))


class basic_publish(AMQHandler):

    def post(self, exchange):
        # TODO headers, properties
        margs = self.args(("body", "", (str, )),
                          ("priority", 0, (maybe_int, )))
        pargs = self.args(("routing_key", "", (str, )),
                           ("mandatory", False, (maybe_bool, )),
                           ("immediate", False, (maybe_bool, )))
        properties = self.args(("delivery_mode", 2, (maybe_int, )))
        content_type, content_encoding = self.get_content_type()

        message = self.amqchannel.prepare_message(
                                           message_data=margs.pop("body"),
                                           content_type=content_type,
                                           content_encoding=content_encoding,
                                           properties=properties,
                                           **margs)
        self.amqchannel.basic_publish(message,
                                      exchange=exchange,
                                      **pargs)
        self.write({"ok": "ok"})


class basic_get(AMQHandler):

    def get(self, queue):
        kwargs = self.args(("no_ack", False, (maybe_bool, )))
        try:
            message = self.amqchannel.basic_get(queue=queue, **kwargs)
            if message:
                message = message_to_dict(
                            self.amqchannel.message_to_python(message))
            self.write(message)
        except KeyError:
            raise HTTPError(404, "Unknown queue: %s" % (queue, ))



class AMQApplication(Application):
    handlers = ((r"/basic/(.+?)/publish/?", basic_publish),
                (r"/basic/(.+?)/get/?", basic_get),
                (r"/exchange/(.+?)/declare/?", exchange_declare),
                (r"/queue/(.+?)/declare/?", queue_declare),
                (r"/queue/(.+?)/bind/(.+?)/?", queue_bind),
    )

    def __init__(self, broker, **kwargs):
        self.broker = broker
        Application.__init__(self, self.handlers, **kwargs)


class AMQServer(object):

    def __init__(self, port=DEFAULT_PORT, broker=None):
        self.port = port
        self.broker = broker or BrokerConnection(transport="memory")

    def run(self):
        app = AMQApplication(self.broker)
        c = self.broker.channel()
        c.queue_declare(queue="foo")
        c.exchange_declare(exchange="foo")
        c.queue_bind(queue="foo", exchange="foo", routing_key="foo")
        m = c.prepare_message("the quick brown fox")
        c.basic_publish(m,
                        exchange="foo",
                        routing_key="foo")
        http_server = httpserver.HTTPServer(app)
        http_server.listen(self.port)
        print("Listening on localhost:%s" % (self.port, ))
        ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    server = AMQServer()
    server.run()


