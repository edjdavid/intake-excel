#!/usr/bin/env python

from setuptools import setup
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-excel',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Excel plugin for Intake',
    url='https://github.com/edjdavid/intake-excel',
    maintainer='Eduardo David Jr',
    maintainer_email='edjdavid@users.noreply.github.com',
    license='BSD',
    py_modules=['intake_excel'],
    packages=['intake_excel'],
    package_data={'': ['*.csv', '*.yml', '*.html']},
    entry_points={
        'intake.drivers': [
            'excel = intake_excel.intake_excel:ExcelSource'
        ]
    },
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    zip_safe=False,
)
