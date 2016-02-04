from setuptools import setup, find_packages


setup(
    name='mongoschema',
    version='3',
    description='lightweight MongoDB ORM on pymongo',
    url='http://github.com/hershaw/mongoschema',
    author='Sam Hopkins',
    author_email='mongoschema@gmail.com',
    packages=find_packages(),
    install_requires=['pymongo']
)
