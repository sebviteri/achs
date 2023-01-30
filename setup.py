from setuptools import setup, find_packages

setup(
    name='achsan',
    version='0.1',
    description='Utilidades para ACHS Analytics',
    author='Sebastian Viteri',
    author_email='sviteriv@achs.cl',
    packages=find_packages(),
    install_requires=['pandas'],

)