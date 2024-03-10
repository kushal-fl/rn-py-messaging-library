from setuptools import setup

setup(
    name='rn_py_messaging_library',
    version='1.0.0',
    packages=[
        'rabbitmq_client',
        'rabbitmq_client/async_connection',
        'rabbitmq_client/consumer',
        'rabbitmq_client/single_threaded_consumer',
    ],
    url='',
    license='',
    author='',
    author_email='',
    description='This Library simplifies messaging queue integrations in Python. '
                'It supports RabbitMQ at this point, having a wrapper over '
                'pika which is the officially recommended client for Rabbit MQ.',
    install_requires=[
        'pika==1.3.2',
        'pydantic==2.6.3',
        'starlette==0.36.3'
    ]
)
