from . import base

from azure.servicebus import *



class Channel(base.StdChannel):

    def exchange_declare(self, exchange, type='direct', durable=False, auto_delete=False, arguments=None, nowait=False, passive=False):
        print exchange


    def queue_declare(self, queue, passive=False, durable=False, exclusive=False, auto_delete=False, arguments=None, nowait=False):
        print queue

    def queue_bind(self, queue, exchange, routing_key='', arguments=None, nowait=False):
        print 'queue_bind', queue, exchange, routing_key

    def prepare_message(self, body, priority=None, content_type=None,
                        content_encoding=None, headers=None, properties=None):
        print 'prepare_message', body

    def basic_publish(self, message, exchange, routing_key='', mandatory=False, immediate=True):
        print 'basic_publish', message


class Transport(base.Transport):
    driver_type = 'sb'
    driver_name = 'azure'

    def establish_connection(self):
        key_name = self.client.transport_options.get('shared_access_key_name', self.client.userid)
        key_value = self.client.transport_options.get('shared_access_key_value', self.client.password)
        return ServiceBusService(
            self.client.hostname,
            shared_access_key_name=key_name,
            shared_access_key_value=key_value)


    def create_channel(self, connection):
        return Channel()