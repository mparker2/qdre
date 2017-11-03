from setuptools import setup

setup(
    name='qdre',
    version='0.1',
    description='kill sge jobs with regex patterns',
    author='matthew parker',
    entry_points={
        'console_scripts': ['qdre = qdre:qdre']
    },
    packages=['qdre'],
    install_requires=['pandas', 'beautifulsoup4']
)
