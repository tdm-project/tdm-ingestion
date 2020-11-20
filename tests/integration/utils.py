import logging
import subprocess
import time


def docker_compose_up(docker_yaml):
    subprocess.check_call(['docker-compose', '-f', docker_yaml, 'up', '-d'])


def docker_compose_restart(docker_yaml, service):
    subprocess.check_call(
        ['docker-compose', '-f', docker_yaml, 'restart', service])


def docker_compose_down(docker_yaml):
    subprocess.check_call(['docker-compose', '-f', docker_yaml, 'down'])


def docker_compose_exec(docker_yaml, service, command):
    subprocess.check_call(
        ["docker-compose", '-f', docker_yaml, "exec", "-T", service, *command])


def try_func(func, sleep, retry, *args, **kwargs):
    """
    func must return True or a similiar value
    """
    passed = False
    counter = 0
    res = None
    while not passed and counter < retry:
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
        raise RuntimeError(f'func {func.__name__} failed with args {args} kwargs {kwargs}')
    return res


def check_docker_logs(docker_yaml, service):
    subprocess.check_output(
        ["docker-compose", '-f', docker_yaml, "logs", service])


def get_service_port(docker_yaml, service, port):
    return subprocess.check_output(
        ['docker-compose', '-f', docker_yaml, 'port', service,
         port]).strip().split(b':')[
        -1].decode()
