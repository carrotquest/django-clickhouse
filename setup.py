from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

requires = []
with open('requirements.txt') as f:
    for line in f.readlines():
        line = line.strip()  # Remove spaces
        line = line.split('#')[0]  # Remove comments
        if line:  # Remove empty lines
            requires.append(line)

setup(
    name='django-clickhouse',
    version='1.0.3',
    packages=['django_clickhouse'],
    package_dir={'': 'src'},
    url='https://github.com/carrotquest/django-clickhouse',
    license='BSD 3-clause "New" or "Revised" License',
    author='Carrot quest',
    author_email='m1ha@carrotquest.io',
    description='Django extension to integrate with ClickHouse database',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=requires
)
