import setuptools


setuptools.setup(
    name='repliqate',
    version='1.0.0',
    description='Data-agnostic SQL to Kafka replication daemon',
    packages=setuptools.find_packages(),
    author='Kevin Lin',
    author_email='developer@kevinlin.info',
    entry_points={
        'console_scripts': [
            'repliqate = repliqate.cmd.main:main',
        ],
    },
)
