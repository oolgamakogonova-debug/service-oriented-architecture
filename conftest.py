import sys
from pathlib import Path

# Тесты этого каталога запускаются отдельной сессией pytest c producer/ в path
# (producer и consumer оба содержат config.py/metrics.py, поэтому их нельзя
# импортировать в одном процессе).
PRODUCER_DIR = Path(__file__).resolve().parents[3] / "producer"
sys.path.insert(0, str(PRODUCER_DIR))
