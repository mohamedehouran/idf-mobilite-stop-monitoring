# 🚌 idf-mobilite-stop-monitoring

IDF Mobilité Stop Monitoring is a powerful Python-based solution for retrieving and processing public transit arrival time data from the IDF Mobilité API. Designed for efficiency and reliability, it supports multi-stop processing and implements smart data formatting to ensure high-quality outputs. Whether you're monitoring transit schedules or analyzing service performance, this tool is built to handle your stop monitoring needs seamlessly.

## ✨ Key Features

### Core Capabilities
- **Optimized parallel processing** : Uses multiple workers to process requests in parallel, reducing latency and improving throughput, ensuring efficient load distribution and API rate-limit compliance
- **Fuzzy matching for locations** : Smart town name matching ensures you get data for the locations you need
- **Smart stop referential management** : Identifies and matches stops based on town selections
- **Robust data formatting** : Transforms complex nested API responses into clean, structured datasets
- **Efficient data storage** : Saves both raw and processed data for maximum flexibility

### Output Data Structure
| Field | Description |
|-------|-------------|
| `recordedattime` | Timestamp when data was recorded |
| `itemidentifier` | Unique identifier for the monitoring record |
| `monitoringref` | Reference code for the monitoring point |
| `lineref` | Transit line identifier |
| `operatorref` | Transit operator reference |
| `directionname` | Direction of travel |
| `destinationref` | Reference ID for the destination |
| `destinationname` | Final destination of the service |
| `vehiclejourneyname` | Name/ID of the specific vehicle journey |
| `journeynote` | Notes related to the journey |
| `vehiclefeatureref` | Reference to vehicle features/capabilities |
| `dataframeref` | Reference to the data frame |
| `datedvehiclejourneyref` | Reference ID for the specific journey on a date |
| `trainnumberref` | Reference number for train services |
| `stoppointname` | Name of the stop point |
| `vehicleatstop` | Indicates if vehicle is currently at stop |
| `destinationdisplay` | Display name for the destination |
| `expectedarrivaltime` | Predicted arrival time |
| `expecteddeparturetime` | Predicted departure time |
| `departurestatus` | Status of the departure (on time, delayed, etc.) |
| `order` | Sequencing order in the journey |
| `aimedarrivaltime` | Scheduled arrival time |
| `aimeddeparturetime` | Scheduled departure time |
| `arrivalstatus` | Status of the arrival (on time, delayed, etc.) |
| `directionref` | Reference code for the direction |
| `destinationshortname` | Abbreviated name of the destination |
| `callnote` | Notes related to the stop call |

## 🚀 Getting Started

### Prerequisites
1. **Install Python (version 3.12 or higher)** : Follow the official instructions at https://python.org/downloads
2. **Obtain an IDF Mobilité API key** : Sign up at https://prim.iledefrance-mobilites.fr/ and add your API key to the `.env` file

### Project Structure
```
idf-stop-monitoring/
├── data/
├── src/
│   ├── config/                           # Configuration files
│   │   ├── app.py                        # Application settings
│   │   ├── config_validator.py           # Configuration validation
│   │   ├── logger.py                     # Logging configuration
│   │   └── stop_monitoring.py            # Stop monitoring configuration
│   │   └── stop_referential.json         # Stop referential
│   ├── utils/                            # Utility functions
│   │   ├── data_retriever.py             # Core data retrieval logic
│   │   └── helpers.py                    # General utilities
│   └── api.py                            # Main entry point of the FastAPI application
├── .env.example                          # Environment variables template
├── .gitignore                            # Git ignore file
├── .pre-commit-config.yaml               # Pre-commit hooks configuration
├── .python-version                       # Python version specification
├── LICENSE                               # License file
├── pyproject.toml                        # Project configuration and dependencies
├── README.md                             # Project documentation
└── uv.lock                               # Dependency lock file
```

### Quick Start Guide
1. **Clone the repository** :
   ```bash
   git clone https://github.com/mohamedehouran/idf-mobilite-stop-monitoring.git
   cd idf-mobilite-stop-monitoring
   ```
2. **Configure environment variables** :
```bash
cp .env.example .env
# Edit .env with your settings
```
3. **Create and activate a virtual environment** : 
```bash
pip install --upgrade uv
uv venv

# Activate the environment
.venv\Scripts\activate        # For Windows
source .venv/bin/activate     # For macOS/Linux

# Install dependencies
uv sync
```
4. **Run the application** :
```bash
uv run uvicorn src.api:app --reload
```
- This command starts the FastAPI application using Uvicorn
- Open your browser and go to `http://localhost:8000/docs` to access the interactive API documentation
5. **Specify the towns** : Use the interactive API documentation to enter a comma-separated list of towns you want to process
6. **Retrieve the output file** : Once the process is complete, the geocoded results will be saved in the `data/` directory. You can also download the results as a CSV file through the API endpoint

## ⚙️ Customization
- **Adjust data processing parameters** in `.env` : Modify the `MAX_WORKERS` value to optimize performance based on your system's capabilities
- **Extend data formatting** in `src/utils/data_retriever.py` : Add additional transformation steps to the `StopMonitoringDataFormatter` class
- **Update Stop Referential data** in `src/config/` : The project uses the referencial data version from May 2025 by default. You can update this by replacing the `stop_referential.json` file in this directory when new data becomes available

## 📈 Use Cases
- **Transit service monitoring** : Track arrival times and service regularity
- **Passenger information systems** : Power real-time arrival displays
- **Transit pattern analysis** : Analyze historical arrival data to identify trends
- **Service disruption detection** : Identify gaps or irregularities in service
- **Multi-modal journey planning** : Combine with other transit data for comprehensive planning

## 📝 Third-Party Services & Licenses
The use of data provided by the IDF Mobilité API is subject to the [IDF Mobilité Terms of Use](https://data.iledefrance-mobilites.fr/terms/terms-and-conditions/). You must comply with these terms when using this project.

## 🤝 Contributing
Contributions are welcome ! Please feel free to submit a Pull Request.

## 📄 License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.