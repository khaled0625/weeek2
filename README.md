# weeek2


## Requirements
- Python 3.9+
- **uv** (Python package & environment manager)

## Installation

### Clone the repository :
```bash
# Clone the repository
git clone https://github.com/khaled0625/weeek2.git
cd weeek2
```
### active the environment and install dependency:
```bash
uv sync
```
## Quick Start
### note make suer you have raw data in this directory **data/raw**
```bash
 cd python_scripts
 uv run run_day1_load.py
 uv run run_day2_clean.py
 uv run run_day3_build_analytics.py
  ```
## to run python notebook
```bash 
uv run jupyter lab notebooks.eda.ipynb
```
