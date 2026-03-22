from typing import Dict
import yaml


def load_yml_configs(config_path: str) -> Dict:
    """Load YAML config file and return as dict."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def validate_running_env(env: str = None) -> str:
    """Validate environment is dev or prod; return lowercase."""
    env = (env or "dev").lower()
    if env not in ["dev", "prod"]:
        raise ValueError(
            f"Invalid environment variable: {env}. Expected 'dev' or 'prod'."
        )
    return env
