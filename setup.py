from setuptools import setup

setup(
    name='esl',
    packages=['core'],
    entry_points={
        'console_scripts': [
            'esl=core.cli:app'
        ],
    },
)
