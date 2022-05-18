from setuptools import setup, find_packages

setup(
    name='esl',
    packages=['core'],
    entry_points={
        'console_scripts': [
            'esl=core.cli:app'
        ],
    },
)
