# Jira Sprint Stats Automator

Automating the capture and visualization of agile metrics from Jira to ensure alignment with native Jira Sprint Reports.

## Features

- **Dynamic Metric Calculation**: Accurately calculates Velocity, Planned Completion %, Bugs In/Out, and Carryover.
- **Interactive Dashboards**: 6 Plotly-based charts for deep sprint insights.
- **Auto-Loading Trends**: Automatically fetches and calculates metrics for past sprints to populate trend charts.
- **Historical Reconstruction**: Uses Jira changelogs to determine issue status at exact sprint end times.
- **Persistent Configuration**: Securely stores Jira credentials and board settings locally.

## Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/B-r-i-a-n-M/agile-stats.git
    cd agile-stats
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the application**:
    ```bash
    streamlit run app.py
    ```

## Usage

1.  Enter your Jira credentials in the sidebar.
2.  Click "Fetch Sprints" to load your board's data.
3.  Select a sprint and click "Fetch & Calculate Metrics".
4.  Explore the interactive charts and detailed issue breakdown.
