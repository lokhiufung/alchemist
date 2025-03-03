# tests/conftest.py
import pytest
import ray

@pytest.fixture(scope="session", autouse=True)
def ray_init_shutdown():
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()