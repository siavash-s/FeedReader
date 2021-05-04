from config import get_conf
import utils

if __name__ == "__main__":
    utils.init_logging(level=get_conf().log_level)
