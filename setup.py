from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    descripcion_larga = fh.read()

setup(
    name='achsan',
    version='0.0.1.dev1',
    description='Utilidades para ACHS Analytics',
    long_description=descripcion_larga, 
    long_description_content_type="text/markdown", 
    author='Sebastian Viteri',
    author_email='sviteriv@achs.cl',
    url='https://github.com/sebviteri/achs', 
    packages=find_packages(),
    install_requires=['pandas', 'pyspark'],
    classifiers=[
        "Development Status :: 1 - Planning", 
        "Intended Audience :: Data Scientists", 
        "Programming Language :: Python :: 3", 
        "Operating System :: Microsoft :: Windows", 
    ]
)