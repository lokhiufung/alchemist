from setuptools import setup, find_packages

def load_requirements(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith('#')]

setup(
    name='alchemist',
    version='0.1.0',
    description='',
    author='lokhiufung',
    author_email='lokhiufung123@gmail.com',
    license='MIT',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(
        where='.',
        include=['alchemist', 'alchemist.*', 'ibapi', 'ibapi.*']
    ),
    include_package_data=True,
    install_requires=load_requirements('requirements.txt'),
    python_requires='>=3.10',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)