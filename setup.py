from setuptools import find_packages, setup

with open('VERSION') as f:
   version = f.read().strip()

setup(
    name='tdm_ingestion',
    version=version,
    packages=find_packages(),
    zip_safe=False,
    install_requires=['requests', 'pyaml', 'jsons', 'stringcase'],
    extras_require={
        'sync': ['confluent-kafka'],
        'async': ['aiokafka']
    },
    scripts=['tdm_ingestion/ingestion.py']
)
