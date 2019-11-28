from setuptools import find_packages, setup

with open('VERSION') as f:
   version = f.read().strip()

setup(
    name='tdm_ingestion',
    version=version,
    packages=find_packages(),
    zip_safe=False,
    install_requires=['requests==2.22.0', 'pyaml==19.4.1', 'jsons==1.0.0', 'stringcase==1.2.0'],
    extras_require={
        'confluent': ['confluent-kafka==1.2.0'],
    },
    scripts=['scripts/kafka_tdmq_ingestion.py',
             'scripts/tdmq_ckan_ingestion.py']
)
