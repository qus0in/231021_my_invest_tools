from setuptools import setup, find_packages
import package_name # TODO : package_name 수정

setup(
    # TODO : package_name 수정
    name='package_name',
    # TODO : package_name 수정
    version=package_name.__version__, 
    # TODO : install_requires 목록 채우기
    install_requires=[],
    packages=find_packages(),
)