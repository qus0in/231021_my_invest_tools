from setuptools import setup, find_packages
import ko_etf_tools

setup(
    name='ko_etf_tools',
    version=ko_etf_tools.__version__, 
    install_requires=['requests', 'pandas', 'scikit-learn'],
    packages=find_packages(),
)