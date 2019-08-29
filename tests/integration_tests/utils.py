import logging
import subprocess
import time


def docker_compose_up():
    subprocess.check_call(['docker-compose', 'up', '-d'])


def docker_compose_down():
    subprocess.check_call(['docker-compose', 'down'])


def try_func(func, sleep, retry, *args, **kwargs):
    """
   func must return True or a similiar value
    """
    passed = False
    counter = 0
    res = None
    while not passed or counter < retry:
        try:
            res = func(*args, **kwargs)
            if res:
                passed = True
                break
        except Exception as ex:
            logging.exception(ex)
        counter += 1
        time.sleep(sleep)
    if not passed:
        raise RuntimeError('func % failed with args %s kwargs %s', func, args,
                           kwargs)
    return res


def check_docker_logs(service):
    subprocess.check_output(["docker-compose", "logs", service])


def get_tdmq_port():
    return subprocess.check_output(
        ['docker-compose', 'port', 'web', '8000']).strip().split(b':')[
        -1].decode()
