# pip_tool_package
> 분석 툴 pip 패키징


* [Github의 새로운 기능, Template repository 에 대해 알아보자](https://velog.io/@bgm537/Github%EC%9D%98-%EC%83%88%EB%A1%9C%EC%9A%B4-%EA%B8%B0%EB%8A%A5-Template-repository-%EC%97%90-%EB%8C%80%ED%95%B4-%EC%95%8C%EC%95%84%EB%B3%B4%EC%9E%90-fsjwpt0x00)
* [[Python] github 저장소를 pip install 설치가 가능하도록 만드는 방법](https://minimin2.tistory.com/189)


```shell
pip install git+https://github.com/qus0in/pip_tool_package.git -q
```
```python
import package_name
package_name.__version__
```