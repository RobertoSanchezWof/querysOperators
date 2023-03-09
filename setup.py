from setuptools import setup, find_packages

setup(
    name='QueryOperators',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'Click',
    ],
    entry_points=["console_scripts = queryOperators:main"]
    )