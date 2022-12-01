from setuptools import setup

setup(
    name='reia',
    packages=[''],
    entry_points={
        'console_scripts': [
            'reia=reia.cli:app'
        ],
    },
)
