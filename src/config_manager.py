import os
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
def load_config():
    
    load_dotenv()
    env_name = os.getenv("environment_name", "local")
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "configs", "config_local_dev.yml")

    if env_name.lower() == 'dataproc':
        config_path = os.path.join(BASE_DIR, "configs", "config_dataproc.yml")

    """Load YAML configuration file."""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config