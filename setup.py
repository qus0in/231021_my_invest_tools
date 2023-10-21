from setuptools import setup, find_packages
import my_invest_tools

setup(
    name='my_invest_tools',
    version=my_invest_tools.__version__, 
    install_requires=['requests', 'pandas', 'scikit-learn'],
    packages=find_packages(),
)