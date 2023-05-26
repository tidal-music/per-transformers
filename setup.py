from glob import glob
from os.path import splitext, basename

from setuptools import setup, find_packages

VERSION = "0.0.1"


def get_requirements():
    with open('requirements.txt') as fp:
        return [x.strip() for x in fp.read().split('\n') if not x.startswith('#')]


with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='transformer',
    version=VERSION,
    description='common transformers used by the tidal personlization team.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tidal-open-source/per-transformers',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    install_requires=get_requirements(),
    include_package_data=True,
    extra_requires={
        'tests': get_requirements()
    },
)
