from config import get_conf
import utils
import dependencies
import signal


def graceful_exit(sig=None, frame=None):
    dependencies.get_main_loop().exit()


signal.signal(signal.SIGTERM, graceful_exit)
signal.signal(signal.SIGINT, graceful_exit)

if __name__ == "__main__":
    utils.init_logging(level=get_conf().log_level.value)
    dependencies.get_main_loop().loop()
