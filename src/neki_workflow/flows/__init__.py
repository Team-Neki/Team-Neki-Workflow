"""워크플로. 하나당 디렉토리 하나이며 @flow와 @task를 함께 둔다.

flow는 flow.py에, task는 역할별 모듈에 나눠 담는다. task 모듈이 flow.py를
import하지 않아야 한다. 여러 워크플로가 함께 쓰는 task는 common에 둔다.
"""
