from setuptools import find_packages, setup

setup(
    name='tdm_ingestion',
    version='0.0.1',
    packages=find_packages(),
    zip_safe=False,
    install_requires=['requests', 'pyaml'],
    extras_require={
        'confluent-kafka': ['confluent-kafka']
    },
    scripts=['tdm_ingestion/ingestion.py']
)
