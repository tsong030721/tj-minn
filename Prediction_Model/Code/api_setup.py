# File to set up any api endpoints
import sys
from pathlib import Path

# Register api source to sys.path to access endpoints
api_path = Path(__file__).resolve().parent.parent / "NBA_API" / "src"
api_path = str(api_path)
if api_path not in sys.path:
    sys.path.append(str(api_path))